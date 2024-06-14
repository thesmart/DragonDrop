import { customAlphabet } from '@npm/nanoid';
import { z, ZodError } from '@x/zod';

export const nanoid = customAlphabet(
  '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
  10,
);

function die(
  msg: string,
  exitCode: number = 1,
) {
  console.error(msg);
  Deno.exit(exitCode);
}

const envSchema = z
  .object({
    HOST: z.string().optional().default('localhost'),
    PORT: z.string().optional().default('8080').pipe(z.coerce.number()),
    S3_REGION: z.string(),
    S3_BUCKET: z.string(),
  });
type Environment = z.infer<typeof envSchema>;

export const { HOST, PORT, S3_REGION, S3_BUCKET } = (() => {
  try {
    return envSchema.parse(Deno.env.toObject());
  } catch (e) {
    if (e instanceof ZodError) {
      die(
        `Invalid environment variable (${e.issues[0].path.join('.')}): ${
          e.issues[0].message
        }`,
      );
    }
    throw e;
  }
})();
