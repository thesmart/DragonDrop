import { PutObjectCommand } from '@npm/aws-sdk/client-s3';
import type { S3Client } from '@npm/aws-sdk/client-s3';
import { nanoid, S3_BUCKET } from './config.ts';
import contentTypes from './mimes.json' with { type: 'json' };
import { createReadStream } from 'node:fs';
import { PassThrough } from 'node:stream';

/**
 * The file extensions for mapping an uploaded file to S3 and properly
 * setting its `Content-type` header.
 */
const supportedFileExtensions = Object.keys(
  contentTypes,
) as (keyof typeof contentTypes)[];

/**
 * Upload a file into S3 using the file system.
 * @param client
 * @param filePath
 * @returns
 */
export async function uploadFile(client: S3Client, filePath: string) {
  // check the file exists
  const fileStat = await Deno.stat(filePath);
  if (!fileStat.isFile) {
    throw new Error(`No file exists at path: ${filePath}`);
  }

  // check the file extension so we can load content type mime
  const extension = supportedFileExtensions.find((ext) => {
    const dotExt = `.${ext}`;
    const index = filePath.lastIndexOf(dotExt);
    return index >= 0 && filePath.length - dotExt.length === index;
  });
  if (!extension) {
    throw new Error(
      `This file doesn't have a recognized extension: ${filePath}`,
    );
  }

  // generate a key for the S3 object
  const key = `${nanoid(4)}/${nanoid(4)}/${nanoid(4)}.${extension}`;
  const passStream = new PassThrough();

  // open a read stream to the file
  const fileStream = createReadStream(filePath);

  // setup the command to write the file to S3
  const cmd = new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: key,
    CacheControl: 'max-age=315360000, immutable',
    ContentLength: fileStat.size,
    ContentType: contentTypes[extension],
    Body: passStream,
  });

  // pipe the data from the file into the stream
  fileStream.pipe(passStream, { end: true });

  // send the data
  await client.send(cmd);

  // return the file key
  return key;
}
