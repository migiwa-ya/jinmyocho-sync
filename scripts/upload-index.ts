import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { readFileSync, existsSync } from "node:fs";
import { defineStaticQL, StaticQLConfig } from "staticql";
import { extractDiff } from "staticql/diff";
import { GitHubDiffProvider } from "staticql/diff/github";
import { FetchRepository } from "staticql/repo/fetch";
import { FsRepository } from "staticql/repo/fs";

const {
  R2_ACCESS,
  R2_SECRET,
  R2_ENDPOINT,
  R2_BUCKET,
  GIT_HEAD_REF,
  GIT_BASE_REF,
  GITHUB_REPO,
  GITHUB_TOKEN,
  CLOUDFLARE_ZONE_ID,
  CLOUDFLARE_CDN_ORIGIN,
  CLOUDFLARE_API_TOKEN,
} = process.env;

if (
  !R2_ACCESS ||
  !R2_SECRET ||
  !R2_ENDPOINT ||
  !R2_BUCKET ||
  !GIT_HEAD_REF ||
  !GIT_BASE_REF ||
  !GITHUB_REPO ||
  !GITHUB_TOKEN ||
  !CLOUDFLARE_ZONE_ID ||
  !CLOUDFLARE_CDN_ORIGIN ||
  !CLOUDFLARE_API_TOKEN
) {
  console.error("[upload-index] env missing");
  process.exit(1);
}

(async () => {
  const res = await fetch(CLOUDFLARE_CDN_ORIGIN + "/staticql.config.json");
  const config: StaticQLConfig = await res.json();

  if (!config) {
    console.error("[upload-index] staticql.config.json not found");
    process.exit(1);
  }
  const githubProvider = new GitHubDiffProvider({
    repo: GITHUB_REPO,
    token: GITHUB_TOKEN,
  });

  const diffEntries = await extractDiff({
    baseRef: GIT_BASE_REF,
    headRef: GIT_HEAD_REF,
    baseDir: "",
    config: {
      ...config,
      sources: {
        shrines: {
          ...config.sources.shrines,
          pattern: "sources/*.md",
        },
        deities: {
          ...config.sources.deities,
          pattern: "sources/*.md",
        },
        cities: {
          ...config.sources.cities,
          pattern: "sources/*.json",
        },
      },
    },
    diffProvider: githubProvider,
  });

  const staticql = defineStaticQL(config)({
    repository: new FetchRepository(CLOUDFLARE_CDN_ORIGIN),
    writeRepository: new FsRepository("./"),
  });

  const changedFiles = await staticql
    .getIndexer()
    .updateIndexesForFiles(diffEntries);

  const client = new S3Client({
    region: "auto",
    endpoint: R2_ENDPOINT,
    credentials: { accessKeyId: R2_ACCESS, secretAccessKey: R2_SECRET },
  });

  for (const path of changedFiles) {
    if (existsSync(path)) {
      const body = readFileSync(path);
      await client.send(
        new PutObjectCommand({ Bucket: R2_BUCKET, Key: path, Body: body })
      );
      console.log("[upload-index] PUT", path);
    } else {
      await client.send(
        new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: path })
      );
      console.log("[upload-index] DELETE", path);
    }
  }

  const files = changedFiles.map((path) => `${CLOUDFLARE_CDN_ORIGIN}/${path}`);

  await fetch(
    `https://api.cloudflare.com/client/v4/zones/${CLOUDFLARE_ZONE_ID}/purge_cache`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${CLOUDFLARE_API_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ files }),
    }
  )
    .then((r) => r.json())
    .then(console.log);
})();
