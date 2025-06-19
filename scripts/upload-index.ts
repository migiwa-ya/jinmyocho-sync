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

  const repoName = GITHUB_REPO.split("/")[1];
  const sourceKey = repoName.replace(/^dataset-/, "");
  const overridePattern = sourceKey === "cities" ? "sources/*.json" : "sources/*.md";
  const diffConfig = {
    ...config,
    sources: {
      [sourceKey]: {
        ...config.sources[sourceKey],
        pattern: overridePattern,
      },
    },
  } as StaticQLConfig;
  const diffEntries = await extractDiff({
    baseRef: GIT_BASE_REF,
    headRef: GIT_HEAD_REF,
    baseDir: "",
    config: diffConfig,
    diffProvider: githubProvider,
  });

  const staticql = defineStaticQL(config)({
    repository: new FetchRepository(CLOUDFLARE_CDN_ORIGIN),
    writeRepository: new FsRepository("./"),
  });

  const changedFiles = await staticql
    .getIndexer({
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
    })
    .updateIndexesForFiles(diffEntries);

  const client = new S3Client({
    region: "auto",
    endpoint: R2_ENDPOINT,
    credentials: { accessKeyId: R2_ACCESS_KEY, secretAccessKey: R2_SECRET_KEY },
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

  if (files.length !== 0) {
    console.log(`[upload-index] PURGE ${files}`);

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
    日: 0,
    月: 1,
    火: 2,
    水: 3,
    木: 4,
    金: 5,
    土: 6,
  };
  return map[weekday] ?? 0;
}

function getStartOfWeek(date: Date): Date {
  const d = new Date(date);
  // 0=Sun, 1=Mon, ...
  const day = d.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  d.setDate(d.getDate() + diff);
  d.setHours(0, 0, 0, 0);
  return d;
}

function addDays(date: Date, days: number): Date {
  const result = new Date(date);
  result.setDate(result.getDate() + days);
  return result;
}

function formatDate(date: Date): string {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${mm}${dd}`;
}

function getCalculatedDateJa(
  festival: FestivalJa,
  referenceDate = new Date()
): Date[] {
  const results: Date[] = [];
  const currentYear = referenceDate.getFullYear();
  const weekStart = getStartOfWeek(referenceDate);
  const weekEnd = addDays(weekStart, 6);

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
      const firstDayOfMonth = new Date(
        referenceDate.getFullYear(),
        開催月 - 1,
        1
      );
      const firstDayWeekday = firstDayOfMonth.getDay();

      const offsetToWeekday = (weekdayNum - firstDayWeekday + 7) % 7;
      const baseDate = new Date(firstDayOfMonth);
      baseDate.setDate(
        baseDate.getDate() + offsetToWeekday + 7 * (開催月第何週 - 1)
      );

      for (let i = 開始オフセット; i <= 終了オフセット; i++) {
        const d = addDays(baseDate, i);
        results.push(d);
      }
    }
  }

  return results;
}

const BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

export function encodeGeohash(
  latitude: number,
  longitude: number,
  precision: number = 12
): string {
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
      if (longitude > mid) {
        ch |= 1 << (4 - bit);
        lon[0] = mid;
      } else {
        lon[1] = mid;
      }
    } else {
      mid = (lat[0] + lat[1]) / 2;
      if (latitude > mid) {
        ch |= 1 << (4 - bit);
        lat[0] = mid;
      } else {
        lat[1] = mid;
      }
    }

    isEven = !isEven;

    if (bit < 4) {
      bit++;
    } else {
      geohash += BASE32[ch];
      bit = 0;
      ch = 0;
    }
  }

  return geohash;
}
