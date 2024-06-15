import { parseArgs } from '@std/cli';
import { S3Client } from '@npm/aws-sdk/client-s3';
import { S3Uploader } from './upload.ts';
import { assert } from '@std/assert';

function die(
  msg: string = 'Usage: deno run --allow-all main.ts [FLAGS] [FILE_PATH]',
  exitCode: number = 1,
) {
  console.error(msg);
  Deno.exit(exitCode);
}

const args = parseArgs(Deno.args);
const s3Region: string = (args.region || Deno.env.get('S3_REGION'))!;
assert(
  s3Region.length,
  'Either set S3_REGION in the environment or pass --region',
);
const s3Bucket: string = (args.bucket || Deno.env.get('S3_BUCKET'))!;
assert(
  s3Bucket.length,
  'Either set S3_BUCKET in the environment or pass --bucket',
);

const s3Client = new S3Client({ region: s3Region });
const filePath = args._[0] as string;
if (typeof filePath !== 'string') {
  die();
}

const uploader = new S3Uploader(s3Client, s3Region, s3Bucket, filePath);
await uploader.uploadFile();

console.info(
  'Success: ',
  `${uploader.url}`,
);
