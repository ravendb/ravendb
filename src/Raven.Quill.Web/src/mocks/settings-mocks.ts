import { http, HttpResponse } from "msw";
import type { LicenseResponse, LogConfigurationResponse, QuillUsageResponse } from "@/api/generated/server-api";
import type { CertificateItem } from "@/api/custom-services/certificates-service";
import { apiHttp } from "./api-http";

export const settingsMocks = {
    feedback: () => apiHttp.post("/api/settings/feedback", ({ response }) => response(204).empty()),
    license: (response: LicenseResponse = sampleLicense) =>
        apiHttp.get("/api/settings/license", ({ response: res }) => res(200).json(response)),
    usage: (response: QuillUsageResponse = sampleQuillUsage) =>
        apiHttp.get("/api/settings/usage", ({ response: res }) => res(200).json(response)),
    certificates: (response: CertificateItem[] = sampleCertificates) =>
        apiHttp.get("/api/settings/certificates/get", ({ response: res }) => res(200).json(response)),
    // The OpenAPI contract only documents the 400 responses for generate (real
    // success is a zip download) and edit (empty 200), so these use plain msw.
    certificatesGenerate: () =>
        http.post(
            "/api/settings/certificates/generate",
            () => new HttpResponse("mock certificate zip", { headers: { "Content-Type": "application/octet-stream" } }),
        ),
    certificatesGenerateError: (message = "A certificate with this name already exists.") =>
        http.post("/api/settings/certificates/generate", () => HttpResponse.json({ error: message }, { status: 400 })),
    certificatesEdit: () => http.post("/api/settings/certificates/edit", () => new HttpResponse(null, { status: 200 })),
    logConfiguration: (response: LogConfigurationResponse = sampleLogConfiguration) =>
        apiHttp.get("/api/settings/logs/configuration", ({ response: res }) => res(200).json(response)),
    // The GET only documents a 200, so a load failure has to go through plain msw.
    logConfigurationError: () =>
        http.get("/api/settings/logs/configuration", () => new HttpResponse(null, { status: 500 })),
    updateLogConfiguration: () =>
        apiHttp.post("/api/settings/logs/configuration", ({ response }) => response(204).empty()),
    updateLogConfigurationError: (message = "'logs.path' '/nope' could not be created or written to.") =>
        apiHttp.post("/api/settings/logs/configuration", ({ response }) => response(400).json({ error: message })),
    // The live change landed and only writing the file failed, which the appliance reports as a 500.
    updateLogConfigurationNotPersisted: () =>
        apiHttp.post("/api/settings/logs/configuration", ({ response }) =>
            response(500).json({
                error:
                    "The log configuration was modified but couldn't be persisted. " +
                    "The configuration will be reverted on restart.",
            }),
        ),
};

export const sampleLogConfiguration: LogConfigurationResponse = {
    logs: {
        path: "/var/lib/quill/logs",
        // The live level differs from the startup one so the "At startup" fact is visibly distinct.
        currentMinLevel: "Debug",
        minLevel: "Info",
        currentFilters: [],
        currentLogFilterDefaultAction: "Neutral",
        archiveAboveSizeInMb: 128,
        maxArchiveDays: 3,
        maxArchiveFiles: null,
        enableArchiveFileCompression: false,
    },
    auditLogs: {
        path: "/var/lib/quill/logs",
        level: "Info",
        archiveAboveSizeInMb: 128,
        maxArchiveDays: 3,
        maxArchiveFiles: null,
        enableArchiveFileCompression: false,
    },
    microsoftLogs: {
        currentMinLevel: "Warn",
        minLevel: "Warn",
    },
    canPersist: true,
};

// What the appliance actually ships as: every optional sink off, nothing customised.
export const sampleShippedLogConfiguration: LogConfigurationResponse = {
    logs: {
        path: null,
        currentMinLevel: "Info",
        minLevel: "Info",
        currentFilters: [],
        archiveAboveSizeInMb: 128,
        maxArchiveDays: 3,
        maxArchiveFiles: null,
        enableArchiveFileCompression: false,
    },
    auditLogs: { path: null, level: "Off" },
    microsoftLogs: { currentMinLevel: "Off", minLevel: "Off" },
    canPersist: true,
};

export const sampleLicense: LicenseResponse = {
    response: {
        errorMessage: "",
        expiration: "2026-07-14T00:00:00Z",
        subscriptionExpiration: "2026-07-14T00:00:00Z",
        expired: false,
        firstServerStartDate: "2026-06-14T00:00:00Z",
        id: "1e1ad2a9-02de-4e2b-a2f2-183ac9b1c04f",
        licensedTo: "Acme Corp",
        status: "Commercial",
        type: "Quill",
        version: "7.2",
    },
    connectivity: {
        statusCode: "OK",
        exception: "",
    },
    plans: [
        {
            slug: "enterprise",
            name: "Enterprise",
            tagline: "Production workloads",
            priceLabel: "Custom",
            priceSuffix: "",
            featured: false,
            features: ["Unlimited apps & writes", "2h SLA support"],
        },
    ],
};

// Databases match sampleApps (demo-shop, support-desk) so the certificates view
// resolves app names for permission entries.
export const sampleCertificates: CertificateItem[] = [
    {
        name: "Server Certificate",
        thumbprint: "1E45C9F8A3B27D604B1347D2E859A0C4F6B8D2E1",
        securityClearance: "ClusterNode",
        notBefore: "2025-11-02T00:00:00Z",
        notAfter: "2027-11-02T00:00:00Z",
        permissions: {},
        disabled: false,
    },
    {
        name: "quill-backend",
        thumbprint: "7A2F0B7C4E91D8356F20A1B9C83D45E6F7180C2D",
        securityClearance: "ValidUser",
        notBefore: "2026-01-10T00:00:00Z",
        notAfter: "2031-01-10T00:00:00Z",
        permissions: { "demo-shop": "Admin", "support-desk": "Admin" },
        disabled: false,
    },
    {
        name: "reporting",
        thumbprint: "C90D1E2F3A4B5C6D7E8F9012A3B4C5D6E7F80913",
        securityClearance: "ValidUser",
        notBefore: "2025-06-20T00:00:00Z",
        notAfter: "2026-06-20T00:00:00Z",
        permissions: { "demo-shop": "Read" },
        disabled: false,
    },
    {
        name: "mobile-gateway",
        thumbprint: "3F8A9B0C1D2E4F5061728394A5B6C7D8E9F00112",
        securityClearance: "ValidUser",
        notBefore: "2025-08-01T00:00:00Z",
        // Expires one week from now so the "About to expire" badge always renders.
        notAfter: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString(),
        permissions: { "demo-shop": "ReadWrite" },
        disabled: false,
    },
    {
        name: "legacy-sync",
        thumbprint: "5B6C7D8E9F0A1B2C3D4E5F60718293A4B5C6D7E8",
        securityClearance: "ValidUser",
        notBefore: "2026-02-01T00:00:00Z",
        notAfter: "2028-02-01T00:00:00Z",
        permissions: { "support-desk": "ReadWrite" },
        disabled: true,
    },
];

// ~30 daily points with a gentle wave so the writes chart has shape.
export const sampleQuillUsage: QuillUsageResponse = {
    byPeriod: Array.from({ length: 30 }, (_, index) => {
        const day = String(index + 1).padStart(2, "0");
        const wave = Math.sin((index / 29) * Math.PI * 2);
        return {
            from: `2026-06-${day}T00:00:00Z`,
            to: `2026-06-${day}T23:59:59Z`,
            usage: Math.round(280000 + wave * 130000),
        };
    }),
    perApplication: [
        {
            topologyId: "topology-1",
            applicationName: "support-copilot",
            from: "2026-06-01T00:00:00Z",
            to: "2026-06-30T23:59:59Z",
            usage: 5200000,
        },
        {
            topologyId: "topology-1",
            applicationName: "orders-sync",
            from: "2026-06-01T00:00:00Z",
            to: "2026-06-30T23:59:59Z",
            usage: 2400000,
        },
        {
            topologyId: "topology-2",
            applicationName: "docs-assistant",
            from: "2026-06-01T00:00:00Z",
            to: "2026-06-30T23:59:59Z",
            usage: 1300000,
        },
    ],
};
