/**
 * Full index rebuild script.
 *
 * Clones dataset repositories, generates all indexes locally,
 * and uploads both the config and index files to R2.
 *
 * Usage:
 *   R2_ACCESS_KEY=... R2_SECRET_KEY=... R2_ENDPOINT=... R2_BUCKET=... \
 *   CLOUDFLARE_ZONE_ID=... CLOUDFLARE_CDN_ORIGIN=... CLOUDFLARE_API_TOKEN=... \
 *   npx tsx scripts/rebuild-index.ts
 */
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import {
  readFileSync,
  existsSync,
  rmSync,
  mkdirSync,
  cpSync,
  readdirSync,
  statSync,
} from "node:fs";
import { resolve, relative, join } from "node:path";
import { execSync } from "node:child_process";
import { defineStaticQL, StaticQLConfig } from "staticql";
import { FsRepository } from "staticql/repo/fs";

const {
  R2_ACCESS_KEY,
  R2_SECRET_KEY,
  R2_ENDPOINT,
  R2_BUCKET,
  CLOUDFLARE_ZONE_ID,
  CLOUDFLARE_CDN_ORIGIN,
  CLOUDFLARE_API_TOKEN,
} = process.env;

if (
  !R2_ACCESS_KEY ||
  !R2_SECRET_KEY ||
  !R2_ENDPOINT ||
  !R2_BUCKET ||
  !CLOUDFLARE_ZONE_ID ||
  !CLOUDFLARE_CDN_ORIGIN ||
  !CLOUDFLARE_API_TOKEN
) {
  console.error("[rebuild-index] env missing");
  process.exit(1);
}

const DATA_REPOS: Record<string, { repo: string; contentDir: string }> = {
  shrines: { repo: "migiwa-ya/dataset-shrines", contentDir: "content/shrines" },
  deities: { repo: "migiwa-ya/dataset-deities", contentDir: "content/deities" },
  cities: { repo: "migiwa-ya/dataset-cities", contentDir: "content/cities" },
};

(async () => {
  const config: StaticQLConfig = JSON.parse(
    readFileSync(resolve("staticql.config.json"), "utf-8")
  );

  const client = new S3Client({
    region: "auto",
    endpoint: R2_ENDPOINT,
    credentials: { accessKeyId: R2_ACCESS_KEY, secretAccessKey: R2_SECRET_KEY },
  });

  // --- 1. Clone repos and assemble local content directory ---
  const workDir = resolve("./work");
  if (existsSync(workDir)) rmSync(workDir, { recursive: true, force: true });
  mkdirSync(workDir, { recursive: true });

  for (const [source, { repo, contentDir }] of Object.entries(DATA_REPOS)) {
    console.log(`[rebuild-index] Cloning ${repo}...`);
    const cloneDir = resolve(workDir, `repo-${source}`);
    execSync(`git clone --depth 1 https://github.com/${repo}.git ${cloneDir}`, {
      stdio: "pipe",
    });

    // Copy sources/ → work/content/{source}/
    const srcDir = join(cloneDir, "sources");
    const destDir = resolve(workDir, contentDir);
    mkdirSync(destDir, { recursive: true });
    cpSync(srcDir, destDir, { recursive: true });
    console.log(`[rebuild-index] ${source}: copied to ${contentDir}`);
  }

  // --- 2. Generate indexes locally ---
  const outputDir = resolve("./output");
  if (existsSync(resolve(outputDir, "index"))) {
    rmSync(resolve(outputDir, "index"), { recursive: true, force: true });
  }

  const staticql = defineStaticQL(config)({
    defaultRepository: new FsRepository(workDir),
    writeRepository: new FsRepository(outputDir),
  });

  console.log("[rebuild-index] Generating full indexes...");
  await staticql.saveIndexes({
    "shrines.nameBigram": (value) => {
      return ngram(String(value["名称"]), 2);
    },
    "shrines.geohash": (value) => {
      if (!value.経度 || !value.緯度) return null;
      return encodeGeohash(value.緯度, value.経度);
    },
    "shrines.festivalDate": (value) => {
      if (!value.祭事 || !value.祭事.length) return [];
      const buf: Date[] = [];
      for (const f of value.祭事) {
        buf.push(...getCalculatedDateJa(f));
      }
      return buf.map((b) => formatDate(b));
    },
    "cities.addressBigram": (value) => {
      return ngram(
        String(value["都道府県"]) +
          String(value["郡"] ?? "") +
          String(value["市区町村"]),
        2
      );
    },
  });
  console.log("[rebuild-index] Index generation completed.");

  // --- 3. Delete old indexes on R2 ---
  console.log("[rebuild-index] Deleting old indexes on R2...");
  let continuationToken: string | undefined;
  do {
    const list = await client.send(
      new ListObjectsV2Command({
        Bucket: R2_BUCKET,
        Prefix: "index/",
        ContinuationToken: continuationToken,
      })
    );
    for (const obj of list.Contents ?? []) {
      if (obj.Key) {
        await client.send(
          new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: obj.Key })
        );
      }
    }
    continuationToken = list.NextContinuationToken;
  } while (continuationToken);

  // --- 4. Upload new indexes ---
  console.log("[rebuild-index] Uploading new indexes...");
  const indexDir = resolve(outputDir, "index");
  const files = walkDir(indexDir);
  for (const filePath of files) {
    const key = "index/" + relative(indexDir, filePath);
    const body = readFileSync(filePath);
    await client.send(
      new PutObjectCommand({ Bucket: R2_BUCKET, Key: key, Body: body })
    );
    console.log("[rebuild-index] PUT", key);
  }

  // --- 5. Upload updated config ---
  const configBody = readFileSync(resolve("staticql.config.json"));
  await client.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: "staticql.config.json",
      Body: configBody,
      ContentType: "application/json",
    })
  );
  console.log("[rebuild-index] PUT staticql.config.json");

  // --- 6. Purge CDN cache ---
  console.log("[rebuild-index] Purging CDN cache...");
  await fetch(
    `https://api.cloudflare.com/client/v4/zones/${CLOUDFLARE_ZONE_ID}/purge_cache`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${CLOUDFLARE_API_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ purge_everything: true }),
    }
  )
    .then((r) => r.json())
    .then(console.log);

  // --- 7. Cleanup ---
  rmSync(workDir, { recursive: true, force: true });

  console.log("[rebuild-index] Done.");
})();

function walkDir(dir: string): string[] {
  const files: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walkDir(full));
    } else {
      files.push(full);
    }
  }
  return files;
}

// --- Shared utilities ---

function ngram(str: string, n: number): string[] {
  if (n <= 0) return [];
  if (str.length < n) return [str];
  const result: string[] = [];
  for (let i = 0; i <= str.length - n; i++) {
    result.push(str.slice(i, i + n));
  }
  return result;
}

type FestivalJa = {
  日付区分: "絶対日付" | "相対日付";
  開催月日?: string;
  開催月?: number;
  開催月第何週?: number;
  開催月何曜日?: string;
  開始オフセット?: number;
  終了オフセット?: number;
};

function weekdayStrToNumber(weekday: string): number {
  const map: Record<string, number> = {
    日: 0, 月: 1, 火: 2, 水: 3, 木: 4, 金: 5, 土: 6,
  };
  return map[weekday] ?? 0;
}

function addDays(date: Date, days: number): Date {
  const result = new Date(date);
  result.setDate(result.getDate() + days);
  return result;
}

function formatDate(date: Date): string {
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${mm}${dd}`;
}

function getCalculatedDateJa(festival: FestivalJa, referenceDate = new Date()): Date[] {
  const results: Date[] = [];
  const currentYear = referenceDate.getFullYear();

  if (festival.日付区分 === "絶対日付" && festival.開催月日) {
    const fullDateStr = `${festival.開催月日}-${currentYear}`;
    const parsed = new Date(fullDateStr);
    if (!isNaN(parsed.getTime())) {
      results.push(parsed);
    }
  } else if (festival.日付区分 === "相対日付") {
    const {
      開催月第何週 = 1,
      開催月何曜日 = "日",
      開始オフセット = 0,
      終了オフセット = 0,
      開催月,
    } = festival;

    if (開催月 === referenceDate.getMonth() + 1) {
      const weekdayNum = weekdayStrToNumber(開催月何曜日);
      const firstDayOfMonth = new Date(referenceDate.getFullYear(), 開催月 - 1, 1);
      const firstDayWeekday = firstDayOfMonth.getDay();
      const offsetToWeekday = (weekdayNum - firstDayWeekday + 7) % 7;
      const baseDate = new Date(firstDayOfMonth);
      baseDate.setDate(baseDate.getDate() + offsetToWeekday + 7 * (開催月第何週 - 1));

      for (let i = 開始オフセット; i <= 終了オフセット; i++) {
        results.push(addDays(baseDate, i));
      }
    }
  }

  return results;
}

const BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

function encodeGeohash(latitude: number, longitude: number, precision: number = 12): string {
  let isEven = true;
  let bit = 0;
  let ch = 0;
  let geohash = "";
  let lat = [-90.0, 90.0];
  let lon = [-180.0, 180.0];

  while (geohash.length < precision) {
    let mid: number;
    if (isEven) {
      mid = (lon[0] + lon[1]) / 2;
      if (longitude > mid) { ch |= 1 << (4 - bit); lon[0] = mid; } else { lon[1] = mid; }
    } else {
      mid = (lat[0] + lat[1]) / 2;
      if (latitude > mid) { ch |= 1 << (4 - bit); lat[0] = mid; } else { lat[1] = mid; }
    }
    isEven = !isEven;
    if (bit < 4) { bit++; } else { geohash += BASE32[ch]; bit = 0; ch = 0; }
  }
  return geohash;
}
