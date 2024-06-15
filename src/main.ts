import { parseArgs } from '@std/cli';
import { S3Client } from '@npm/aws-sdk/client-s3';
import { S3_REGION } from './config.ts';
import { uploadFile } from './upload.ts';

function die(
  msg: string = 'Usage: deno task script [FLAGS] [COMMAND]',
  exitCode: number = 1,
) {
  console.error(msg);
  Deno.exit(exitCode);
}

const args = parseArgs(Deno.args);

const s3Client = new S3Client({ region: S3_REGION });

const commands = {
  upload: async () => {
    const filePath = args._[1] as string;
    if (typeof filePath !== 'string') {
      die('Usage: deno task script [FLAGS] upload [FILE_PATH]');
    }
    console.info(`Writing file (${filePath}) to S3 ... `);
    const { url } = await uploadFile(s3Client, filePath);
    console.info(
      'Success: ',
      `${url}`,
    );
  },
};

if (typeof args._[0] !== 'string') {
  die();
}
const command = args._[0] as keyof typeof commands;
commands[command] ? await commands[command]() : die();
