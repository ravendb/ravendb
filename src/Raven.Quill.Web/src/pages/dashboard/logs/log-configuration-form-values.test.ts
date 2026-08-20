import { describe, expect, it } from "vitest";
import type { LogConfigurationResponse } from "@/api/generated/server-api";
import {
    describeRotation,
    isFrameworkCaptureOn,
    logConfigurationFormSchema,
    toFormValues,
    toUpdateRequest,
    type LogConfigurationFormValues,
} from "./log-configuration-form-values";

function configuration(overrides: Partial<LogConfigurationResponse> = {}): LogConfigurationResponse {
    return {
        logs: { path: "/var/lib/quill/logs", minLevel: "Info", currentMinLevel: "Debug", archiveAboveSizeInMb: 128 },
        auditLogs: { path: null, level: "Off" },
        microsoftLogs: { minLevel: "Warn", currentMinLevel: "Warn" },
        canPersist: true,
        ...overrides,
    };
}

const values: LogConfigurationFormValues = {
    logDirectory: "/var/lib/quill/logs",
    minLevel: "Warn",
    frameworkMinLevel: "Error",
    shouldPersist: true,
};

describe("logDirectory validation", () => {
    function validate(logDirectory: string) {
        return logConfigurationFormSchema.safeParse({ ...values, logDirectory });
    }

    // A relative directory resolves against /app/web inside the container image, so the log file
    // an operator meant to keep is destroyed by the next recreate - and the save still succeeds.
    it.each(["logs", "./logs", "../logs", "var/lib/quill/logs"])("rejects the relative path %j", (directory) => {
        expect(validate(directory).success).toBe(false);
    });

    it.each(["/var/lib/quill/logs", "/logs", "C:\\quill\\logs", "C:/quill/logs", "\\\\host\\share\\logs"])(
        "accepts the absolute path %j",
        (directory) => {
            expect(validate(directory).success).toBe(true);
        },
    );

    it("still accepts empty, which is how the file sink is switched off", () => {
        expect(validate("").success).toBe(true);
    });

    it("trims before deciding, so a padded absolute path is not read as relative", () => {
        const result = validate("  /var/lib/quill/logs  ");
        expect(result.success).toBe(true);
        expect(result.data?.logDirectory).toBe("/var/lib/quill/logs");
    });

    it("explains what to type instead", () => {
        expect(validate("logs").error?.issues[0]?.message).toBe(
            "Enter an absolute path, for example /var/lib/quill/logs",
        );
    });
});

describe("toFormValues", () => {
    it("seeds the level from currentMinLevel, not the startup value", () => {
        expect(toFormValues(configuration()).minLevel).toBe("Debug");
    });

    it("maps a null path to an empty directory", () => {
        expect(toFormValues(configuration({ logs: { path: null } })).logDirectory).toBe("");
    });

    it("falls back when the server reports a level the client does not know", () => {
        expect(toFormValues(configuration({ logs: { currentMinLevel: "Chatty" } })).minLevel).toBe("Info");
    });

    it("starts persist on only when the appliance can write the configuration back", () => {
        expect(toFormValues(configuration()).shouldPersist).toBe(true);
        expect(toFormValues(configuration({ canPersist: false })).shouldPersist).toBe(false);
    });
});

describe("toUpdateRequest", () => {
    // Every one of these guards a server default that would otherwise be applied silently.
    it("always sends a complete logs block", () => {
        expect(toUpdateRequest(values, configuration()).logs).toEqual({
            path: "/var/lib/quill/logs",
            minLevel: "Warn",
        });
    });

    it("sends an empty path rather than omitting it, which is how the file sink is switched off", () => {
        const request = toUpdateRequest({ ...values, logDirectory: "" }, configuration());
        expect(request.logs).toEqual({ path: "", minLevel: "Warn" });
    });

    it("sends a complete microsoftLogs block while framework capture is on", () => {
        expect(toUpdateRequest(values, configuration()).microsoftLogs).toEqual({ minLevel: "Error" });
    });

    it("omits microsoftLogs entirely while framework capture is off", () => {
        const off = configuration({ microsoftLogs: { minLevel: "Off", currentMinLevel: "Off" } });
        expect(toUpdateRequest(values, off).microsoftLogs).toBeNull();
    });

    it("never asks to persist when the appliance cannot", () => {
        expect(toUpdateRequest(values, configuration({ canPersist: false })).persist).toBe(false);
    });

    it("passes the operator's persist choice through when it can", () => {
        expect(toUpdateRequest(values, configuration()).persist).toBe(true);
        expect(toUpdateRequest({ ...values, shouldPersist: false }, configuration()).persist).toBe(false);
    });
});

describe("isFrameworkCaptureOn", () => {
    it("is off for Off and for a missing level", () => {
        expect(isFrameworkCaptureOn(configuration({ microsoftLogs: { currentMinLevel: "Off" } }))).toBe(false);
        expect(isFrameworkCaptureOn(configuration({ microsoftLogs: {} }))).toBe(false);
        expect(isFrameworkCaptureOn(configuration())).toBe(true);
    });
});

describe("describeRotation", () => {
    it("lists the settings the target reports", () => {
        expect(describeRotation({ archiveAboveSizeInMb: 128, maxArchiveDays: 3 })).toBe("128 MB per file, 3 days kept");
    });

    it("treats a zero size as unknown, because a setting NLog rejected also reports zero", () => {
        expect(describeRotation({ archiveAboveSizeInMb: 0 })).toBe("Not reported");
    });
});
