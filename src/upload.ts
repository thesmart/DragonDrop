import { assert } from '@std/assert';
import {
  CompletedPart,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  PutObjectCommand,
  UploadPartCommand,
} from '@npm/aws-sdk/client-s3';
import type { S3Client } from '@npm/aws-sdk/client-s3';
import { customAlphabet } from '@npm/nanoid';
import contentTypes from './mimes.json' with { type: 'json' };
import { PromiseQueue } from './promise-queue.ts';

/**
 * The file extensions for mapping an uploaded file to S3 and properly
 * setting its `Content-type` header.
 */
const supportedFileExtensions = Object.keys(
  contentTypes,
) as (keyof typeof contentTypes)[];

/**
 * Generate a random object key and lookup the mime type based on filePath
 * extension.
 */
function genKeyAndContentType(
  filePath: string,
): { s3ObjectKey: string; extension: string; contentType: string } {
  // check the file extension so we can load content type mime
  const extension = supportedFileExtensions.find((ext) => {
    const dotExt = `.${ext}`;
    const index = filePath.lastIndexOf(dotExt);
    return index >= 0 && filePath.length - dotExt.length === index;
  });
  if (!extension) {
    throw new Error(
      `This file doesn't have a recognized extension: (${filePath})`,
    );
  }
  const contentType = contentTypes[extension];

  // generate a key for the S3 object
  const s3ObjectKey = `${nanoid(4)}/${nanoid(4)}/${nanoid(4)}.${extension}`;

  return { s3ObjectKey, extension, contentType };
}

/**
 * Generate unique ids.
 */
const nanoid = customAlphabet(
  '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
  10,
);

interface UploadResult {
  s3ObjectKey: string;
  eTag: string;
  s3Region: string;
  s3Bucket: string;
  url: string;
}

interface UploadPartResult {
  s3UploadId: string;
  partNumber: number;
  eTag: string;
}

/**
 * Manages uploads to S3.
 */
export class S3Uploader {
  s3Region: string;
  s3Bucket: string;
  s3Client: S3Client;
  s3ObjectKey: string;
  filePath: string;
  fileExtension: string;
  fileContentType: string;
  fsFile: Deno.FsFile;
  fsFileInfo: Deno.FileInfo;
  concurrency: number = 8; // concurrent uploads
  chunkByteSize: number = 1024 * 10_000; // 10 MB

  constructor(
    client: S3Client,
    region: string,
    bucket: string,
    filePath: string,
  ) {
    this.s3Region = region;
    this.s3Bucket = bucket;
    this.s3Client = client;
    // generate remote file metadata
    const fileMeta = genKeyAndContentType(filePath);
    this.s3ObjectKey = fileMeta.s3ObjectKey;
    this.filePath = filePath;
    this.fileExtension = fileMeta.extension;
    this.fileContentType = fileMeta.contentType;
    this.fsFile = Deno.openSync(this.filePath, { read: true });
    // check the file exists
    this.fsFileInfo = this.fsFile.statSync();
    if (!this.fsFileInfo.isFile) {
      throw new Error(`No file exists at path (${filePath}).`);
    } else if (!this.fsFileInfo.size) {
      throw new Error(`Unable to upload a zero byte file (${filePath}).`);
    }
  }

  get url() {
    return `https://${this.s3Bucket}.s3.${this.s3Region}.amazonaws.com/${this.s3ObjectKey}`;
  }

  async uploadFile(): Promise<UploadResult> {
    // 5MB is the minimum for multipart uploads
    if (this.fsFileInfo.size >= 1024 * 5_000) {
      return await this.multiPartUpload();
    }

    // read file into memory
    const buffer = new Uint8Array(this.fsFileInfo.size);
    await this.fsFile.read(buffer);

    const cmd = new PutObjectCommand({
      Bucket: this.s3Bucket,
      Key: this.s3ObjectKey,
      CacheControl: 'max-age=315360000, immutable',
      ContentType: this.fileContentType,
      Body: buffer,
    });
    const results = await this.s3Client.send(cmd);
    results;
    return {
      s3Region: this.s3Region,
      s3Bucket: this.s3Bucket,
      s3ObjectKey: this.s3ObjectKey,
      eTag: results.ETag!,
      url: this.url,
    };
  }

  private async multiPartUpload(): Promise<UploadResult> {
    // startup a multipart upload
    console.info(
      `Starting multipart upload of ${this.fsFileInfo.size} bytes ...`,
    );
    const createCommand = new CreateMultipartUploadCommand({
      Bucket: this.s3Bucket,
      Key: this.s3ObjectKey,
      CacheControl: 'max-age=315360000, immutable',
      ContentType: this.fileContentType,
    });
    const { UploadId: s3UploadId } = await this.s3Client.send(createCommand);

    // setup concurrent uploads
    const queue = new PromiseQueue<UploadPartResult>(this.concurrency);
    const totalPartCount = Math.ceil(this.fsFileInfo.size / this.chunkByteSize);
    let remainingBytes = this.fsFileInfo.size;
    let currPartNumber = 0;

    while (remainingBytes > 0) {
      ++currPartNumber;
      const currPartByteSize = remainingBytes >= this.chunkByteSize
        ? this.chunkByteSize
        : remainingBytes;
      remainingBytes -= currPartByteSize;
      queue.add(this.createPartUploadFn(
        s3UploadId!,
        currPartNumber,
        currPartByteSize,
        totalPartCount,
      ));
    }

    // wait for all parts to upload
    const results = await queue.execute();
    const multipartUploadManifest: CompletedPart[] = results.map((r) => ({
      PartNumber: r.partNumber,
      ETag: r.eTag,
    }));

    // complete the upload
    console.info('All parts uploaded, now completing the upload ...');
    const completeCommand = new CompleteMultipartUploadCommand({
      Bucket: this.s3Bucket,
      Key: this.s3ObjectKey,
      UploadId: s3UploadId,
      MultipartUpload: {
        Parts: multipartUploadManifest,
      },
    });

    const result = await this.s3Client.send(completeCommand);
    return {
      s3Region: this.s3Region,
      s3Bucket: this.s3Bucket,
      s3ObjectKey: this.s3ObjectKey,
      eTag: result.ETag!,
      url: this.url,
    };
  }

  private createPartUploadFn(
    s3UploadId: string,
    partNumber: number,
    partByteSize: number,
    totalPartCount: number,
  ): () => Promise<UploadPartResult> {
    return async (): Promise<UploadPartResult> => {
      console.info(
        `\tUploading part ${partNumber} of ${totalPartCount} (${partByteSize} bytes)...`,
      );
      // allocate a buffer
      const buffer = new Uint8Array(partByteSize);
      // read a file chunk into the buffer
      const bytesRead = await this.fsFile.read(buffer);
      assert(
        bytesRead,
        'Expected to read more bytes, stat size differs from read size.',
      );

      // upload the chunk to S3
      const cmd = new UploadPartCommand({
        Bucket: this.s3Bucket,
        Key: this.s3ObjectKey,
        PartNumber: partNumber,
        UploadId: s3UploadId,
        Body: buffer,
      });

      const uploadPartPromise = this.s3Client.send(cmd).then((result) => {
        console.info(
          `\tSuccessfully uploaded part ${partNumber} of ${totalPartCount} (${bytesRead} bytes).`,
        );
        return {
          s3UploadId,
          partNumber,
          eTag: result.ETag!,
        } as UploadPartResult;
      });
      return uploadPartPromise;
    };
  }
}
