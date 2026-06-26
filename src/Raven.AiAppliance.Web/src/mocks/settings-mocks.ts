import type { LicenseResponse, MonthlyWritesResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const settingsMocks = {
    license: (response: LicenseResponse = sampleLicense) =>
        apiHttp.get("/api/settings/license", ({ response: res }) => res(200).json(response)),
    usage: (response: MonthlyWritesResponse = sampleMonthlyWrites) =>
        apiHttp.get("/api/settings/usage", ({ response: res }) => res(200).json(response)),
};

export const sampleLicense: LicenseResponse = {
    state: "healthy",
    tier: "Trial",
    daysLeft: 19,
    daysElapsed: 11,
    trialLengthDays: 30,
    trialStartedLabel: "Jun 14",
    trialEndsLabel: "Jul 14, 2026",
    graceHoursLeft: null,
    graceEndsLabel: null,
    api: "api.ravendb.ai",
    apiHealthy: true,
    connectivityOK: true,
    tierHealthy: true,
    lastRefreshedLabel: "2 min ago",
    plans: [
        {
            slug: "developer",
            name: "Developer",
            tagline: "Single-app, low-volume",
            priceLabel: "$49",
            priceSuffix: "/mo",
            featured: false,
            features: ["1 app, 100K writes/mo", "BYO LLM key", "Community support"],
        },
        {
            slug: "team",
            name: "Team",
            tagline: "Production workloads",
            priceLabel: "$499",
            priceSuffix: "/mo",
            featured: true,
            features: ["5 apps, 2M writes/mo", "BYO LLM key + local Ollama", "Priority support"],
        },
    ],
    includes: ["Full instance — no caps", "All channels", "All AI providers"],
    stops: null,
    keeps: null,
};

// ~30 daily points with a gentle wave so the Writes sparkline has shape.
export const sampleMonthlyWrites: MonthlyWritesResponse = {
    days: Array.from({ length: 30 }, (_, index) => {
        const day = index + 1;
        const wave = Math.sin((index / 29) * Math.PI * 2);
        return {
            label: `Jun ${day}`,
            date: `2026-06-${String(day).padStart(2, "0")}`,
            writes: Math.round(280000 + wave * 130000),
        };
    }),
    monthlyQuota: 50000000,
    monthlyUsed: 8900000,
    monthLabel: "June 2026",
    quotaResetsOn: "Jul 1, 2026",
    trialDaysLeft: 19,
    isCurrent: true,
};
