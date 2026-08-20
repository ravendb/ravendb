import { z } from "zod";
import type { LogConfigurationResponse, LogLevel, UpdateLogConfigurationRequest } from "@/api/generated/server-api";

// In the server's order. `satisfies` keeps the list honest against the generated LogLevel union.
export const LOG_LEVELS = [
    "Trace",
    "Debug",
    "Info",
    "Warn",
    "Error",
    "Fatal",
    "Off",
] as const satisfies readonly LogLevel[];

// The appliance resolves a relative directory against its own install directory, /app/web, which
// lives in the container image rather than on the volume - so logs written there are destroyed by
// the next recreate and the appliance still reports the save as a success. Rooted paths only.
// Windows shapes are accepted too, for a Raven.Quill run outside the container during development.
const ROOTED_DIRECTORY = /^(\/|[A-Za-z]:[\\/]|\\\\)/;

export const logConfigurationFormSchema = z.object({
    // An empty directory means the file sink is off, which is how the server reads a blank path.
    logDirectory: z
        .string()
        .trim()
        .refine((directory) => directory === "" || ROOTED_DIRECTORY.test(directory), {
            message: "Enter an absolute path, for example /var/lib/quill/logs",
        }),
    minLevel: z.enum(LOG_LEVELS),
    // Maps onto microsoftLogs, which covers both the Microsoft.* and System.* loggers. All seven
    // levels are accepted so a server already reporting Off can be seeded; only the select is narrower.
    frameworkMinLevel: z.enum(LOG_LEVELS),
    shouldPersist: z.boolean(),
});

export type LogConfigurationFormValues = z.infer<typeof logConfigurationFormSchema>;

/** The server's own gate: it rejects a microsoftLogs block whenever framework capture is off. */
export function isFrameworkCaptureOn(configuration: LogConfigurationResponse): boolean {
    const level = configuration.microsoftLogs.currentMinLevel;
    return level != null && level !== "Off";
}

export function toFormValues(configuration: LogConfigurationResponse): LogConfigurationFormValues {
    return {
        logDirectory: configuration.logs.path ?? "",
        // currentMinLevel is the live level. minLevel is whatever the appliance started with, and
        // stays there even after a persisted change, so it is only ever shown as a read-only fact.
        minLevel: configuration.logs.currentMinLevel ?? "Info",
        frameworkMinLevel: configuration.microsoftLogs.currentMinLevel ?? "Off",
        // A settings page that silently forgets on the next restart is the worse failure, so this
        // starts on wherever the appliance can actually write it back.
        shouldPersist: configuration.canPersist,
    };
}

export function toUpdateRequest(
    values: LogConfigurationFormValues,
    configuration: LogConfigurationResponse,
): UpdateLogConfigurationRequest {
    return {
        // Sent on every save and always complete. Inside a block the server fills absent fields
        // with its own defaults rather than keeping the current value - an omitted minLevel here
        // would silently reset the level to Info. Sending it always also means the "neither block"
        // 400 is unreachable.
        logs: {
            path: values.logDirectory,
            minLevel: values.minLevel,
        },
        // All or nothing: null leaves framework logging alone. It is the only safe value while
        // capture is off, because the server answers 400 for a block it cannot apply, and an
        // absent minLevel inside the block would mean Trace rather than "keep current".
        microsoftLogs: isFrameworkCaptureOn(configuration) ? { minLevel: values.frameworkMinLevel } : null,
        // Re-checked against the server's answer so the 409 stays unreachable even if the switch
        // is somehow on without a writable configuration file.
        persist: configuration.canPersist && values.shouldPersist,
    };
}

type RotationFields = {
    archiveAboveSizeInMb?: number;
    maxArchiveDays?: number | null;
    maxArchiveFiles?: number | null;
    enableArchiveFileCompression?: boolean;
};

/**
 * A size of 0 means unknown, not "no rotation": a target NLog could not configure reports -1 bytes,
 * which divides down to 0 MB, so a broken setting is indistinguishable from an unset one.
 */
export function describeRotation(rotation: RotationFields): string {
    const parts = [
        rotation.archiveAboveSizeInMb ? `${rotation.archiveAboveSizeInMb.toLocaleString()} MB per file` : null,
        rotation.maxArchiveDays ? `${rotation.maxArchiveDays} days kept` : null,
        rotation.maxArchiveFiles ? `${rotation.maxArchiveFiles} files kept` : null,
        rotation.enableArchiveFileCompression ? "compressed" : null,
    ].filter((part) => part !== null);

    return parts.length > 0 ? parts.join(", ") : "Not reported";
}
