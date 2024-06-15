import {
  CompletedPart,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  PutObjectCommand,
  UploadPartCommand,
} from '@npm/aws-sdk/client-s3';
import { S3Client } from '@npm/aws-sdk/client-s3';
import { assert } from '@std/assert';
import { nanoid, S3_BUCKET, S3_REGION } from './config.ts';
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

function s3Url(region: string, bucket: string, objectKey: string) {
  return `https://${bucket}.s3.${region}.amazonaws.com/${objectKey}`;
}

interface UploadResult {
  s3ObjectKey: string;
  eTag: string;
  s3Region: string;
  s3Bucket: string;
  url: string;
}

/**
 * Upload a file to S3.
 * If the file is above 5MB, it will be sent concurrently in chunks.
 */
export async function uploadFile(
  client: S3Client,
  filePath: string,
  concurrency: number = 8,
  chunkByteSize: number = 1024 * 10_000, // 10 MB
): Promise<UploadResult> {
  // check the file exists
  const file = await Deno.open(filePath, { read: true });
  const fileStat = await file.stat();
  if (!fileStat.isFile) {
    throw new Error(`No file exists at path (${filePath})`);
  } else if (!fileStat.size) {
    throw new Error(`Unable to upload a zero byte file (${filePath})`);
  }

  // generate remote file metadata
  const { s3ObjectKey, contentType } = genKeyAndContentType(filePath);

  if (fileStat.size > 1024 * 5_000) {
    return await multiPartUpload(
      client,
      file,
      fileStat,
      s3ObjectKey,
      contentType,
      concurrency,
      chunkByteSize,
    );
  }

  // read file into memory
  const buffer = new Uint8Array(fileStat.size);
  await file.read(buffer);

  const cmd = new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: s3ObjectKey,
    CacheControl: 'max-age=315360000, immutable',
    ContentType: contentType,
    Body: buffer,
  });
  const results = await client.send(cmd);
  results;
  return {
    s3Region: S3_REGION,
    s3Bucket: S3_BUCKET,
    s3ObjectKey: s3ObjectKey,
    eTag: results.ETag!,
    url: s3Url(S3_REGION, S3_BUCKET, s3ObjectKey),
  };
}

interface UploadPartResult {
  s3ObjectKey: string;
  s3UploadId: string;
  partNumber: number;
  eTag: string;
}

/**
 * A function that returns a function that uploads a part of a file.
 */
function createUploadPartFn(
  client: S3Client,
  file: Deno.FsFile,
  s3ObjectKey: string,
  s3UploadId: string,
  partNumber: number,
  partByteSize: number,
  totalPartCount: number,
) {
  return async (): Promise<UploadPartResult> => {
    console.info(
      `\tUploading part ${partNumber} of ${totalPartCount} (${partByteSize} bytes)...`,
    );
    // allocate a buffer
    const buffer = new Uint8Array(partByteSize);
    // read a file chunk into the buffer
    const bytesRead = await file.read(buffer);
    assert(
      bytesRead,
      'Expected to be able to read more bytes, stat size differs from read size.',
    );

    // upload the chunk to S3
    const cmd = new UploadPartCommand({
      Bucket: S3_BUCKET,
      Key: s3ObjectKey,
      PartNumber: partNumber,
      UploadId: s3UploadId,
      Body: buffer,
    });

    const uploadPartPromise = client.send(cmd).then((result) => {
      console.info(
        `\tSuccessfully uploaded part ${partNumber} of ${totalPartCount} (${bytesRead} bytes).`,
      );
      return {
        s3ObjectKey,
        s3UploadId,
        partNumber,
        eTag: result.ETag!,
      } as UploadPartResult;
    });
    return uploadPartPromise;
  };
}

/**
 * Upload a file into S3 using the file system.
 */
export async function multiPartUpload(
  client: S3Client,
  file: Deno.FsFile,
  fileStat: Deno.FileInfo,
  s3ObjectKey: string,
  contentType: string,
  concurrency: number,
  chunkByteSize: number = 1024 * 10_000, // 10 MB
): Promise<UploadResult> {
  // startup a multipart upload
  const createCommand = new CreateMultipartUploadCommand({
    Bucket: S3_BUCKET,
    Key: s3ObjectKey,
    CacheControl: 'max-age=315360000, immutable',
    ContentType: contentType,
  });
  const { UploadId: s3UploadId } = await client.send(createCommand);

  // setup concurrent uploads
  const queue = new PromiseQueue<UploadPartResult>(concurrency);
  const totalPartCount = Math.ceil(fileStat.size / chunkByteSize);
  let remainingBytes = fileStat.size;
  let currPartNumber = 0;

  while (remainingBytes > 0) {
    ++currPartNumber;
    const currPartSize = remainingBytes >= chunkByteSize
      ? chunkByteSize
      : remainingBytes;
    remainingBytes -= currPartSize;
    queue.add(createUploadPartFn(
      client,
      file,
      s3ObjectKey,
      s3UploadId!,
      currPartNumber,
      currPartSize,
      totalPartCount,
    ));
  }

  console.info(
    `Starting upload of ${fileStat.size} bytes over ${totalPartCount} parts ...`,
  );
  const results = await queue.execute();
  const multipartUploadManifest: CompletedPart[] = results.map((r) => ({
    PartNumber: r.partNumber,
    ETag: r.eTag,
  }));

  // complete the upload
  console.info('All parts uploaded, now completing the upload ...');
  const completeCommand = new CompleteMultipartUploadCommand({
    Bucket: S3_BUCKET,
    Key: s3ObjectKey,
    UploadId: s3UploadId,
    MultipartUpload: {
      Parts: multipartUploadManifest,
    },
  });

  const result = await client.send(completeCommand);
  return {
    s3ObjectKey: result.Key!,
    eTag: result.ETag!,
    s3Bucket: result.Bucket!,
    s3Region: S3_REGION,
    url: result.Location!,
  };
}
