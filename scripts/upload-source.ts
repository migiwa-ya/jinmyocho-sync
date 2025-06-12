import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { GitHubDiffProvider } from "staticql/diff/github";

const {
  R2_ACCESS_KEY,
  R2_SECRET_KEY,
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
  !R2_ACCESS_KEY ||
  !R2_SECRET_KEY ||
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
  console.error("[upload-source] env missing");
  process.exit(1);
}

(async () => {
  const githubProvider = new GitHubDiffProvider({
    repo: GITHUB_REPO,
    token: GITHUB_TOKEN,
  });

  const changedFiles = await githubProvider.diffLines(
    GIT_BASE_REF,
    GIT_HEAD_REF
  );

  if (changedFiles.length === 0) {
    console.log(`[upload-source] nothing to upload`);
    process.exit(0);
  }

  const client = new S3Client({
    region: "auto",
    endpoint: R2_ENDPOINT,
    credentials: { accessKeyId: R2_ACCESS_KEY, secretAccessKey: R2_SECRET_KEY },
  });

  const pathMap = {
    "migiwa-ya/dataset-shrines": "content/shrines",
    "migiwa-ya/dataset-deities": "content/deities",
    "migiwa-ya/dataset-cities": "content/cities",
  };

  const githubPath = "sources";

  const mimeMap = {
    "migiwa-ya/dataset-shrines": "text/markdown; charset=utf-8",
    "migiwa-ya/dataset-deities": "text/markdown; charset=utf-8",
    "migiwa-ya/dataset-cities": "application/json",
  };

  for (const { status, path } of changedFiles) {
    const r2Path = path.replace(githubPath, pathMap[GITHUB_REPO]);

    if (status !== "D") {
      const body = await githubProvider.gitShow(GIT_HEAD_REF, path);
      await client.send(
        new PutObjectCommand({
          Bucket: R2_BUCKET,
          Key: r2Path,
          Body: body,
          ContentType: mimeMap[GITHUB_REPO],
        })
      );
      console.log("[upload-source] PUT", r2Path);
    } else {
      await client.send(
        new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: r2Path })
      );
      console.log("[upload-source] DELETE", r2Path);
    }
  }

  const files = changedFiles.map(
    ({ path }) =>
      `${CLOUDFLARE_CDN_ORIGIN}/${path.replace(
        githubPath,
        pathMap[GITHUB_REPO]
      )}`
  );

  if (files.length !== 0) {
    console.log(`[upload-source] PURGE ${files}`);

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
  }
})();
