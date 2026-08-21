import type { LogConfigurationResponse, LogLevel } from "@/api/generated/server-api";
import type { StatusTone } from "@/components/data/status-indicator";
import type { LogConfigurationFormValues } from "./log-configuration-form-values";

/**
 * The draft, not the server, drives every state on the page. A settings form that reports the live
 * value while showing an edited one contradicts itself - picking Off used to leave the appliance
 * card reading "Active" right above a warning that nothing is written. When the form is clean the
 * two are the same, so there is nothing to disambiguate.
 */
export type LogDestination = {
    key: string;
    label: string;
    value: string;
    tone: StatusTone;
    /** Explains what the destination is and what its current value means - two words cannot. */
    hint: string;
};

/** Off is the only level that silences stdout, which is why it is worth its own state everywhere. */
export function isAllLoggingOff(values: LogConfigurationFormValues): boolean {
    return values.minLevel === "Off";
}

export type LogAttention = { tone: StatusTone; label: string };

export type LogDrift = { runningLevel: LogLevel; startupLevel: LogLevel };

/**
 * The level the appliance booted with, but only when it no longer matches the running one: the two
 * agreeing is the normal case and says nothing. Both loggers report the pair, so both get the same
 * treatment - the old layout printed each as a permanent "At startup" fact instead.
 */
export function describeDrift(levels: {
    minLevel?: LogLevel | null;
    currentMinLevel?: LogLevel | null;
}): LogDrift | null {
    const { minLevel: startupLevel, currentMinLevel: runningLevel } = levels;
    return startupLevel != null && runningLevel != null && startupLevel !== runningLevel
        ? { runningLevel, startupLevel }
        : null;
}

/**
 * Badges mark exceptions only - a healthy sink returns null and shows nothing.
 *
 * The destinations strip already states where output goes, so a green badge beside a filled control
 * restates what the control itself says, and a page where every row carries one leaves green meaning
 * nothing. Same rule `status-indicator.tsx` applies to its icons: a badge should read as "this needs
 * you".
 *
 * The appliance group has no badge at all: Off is the only state worth flagging there, and the alert
 * inside the level row already says it in full.
 *
 * Only when the field contradicts itself: a directory is set, so the control implies a file is being
 * written, but the level is Off so nothing reaches it. An empty directory needs no badge - the empty
 * field is already the statement that no file is written.
 */
export function describeFileAttention(values: LogConfigurationFormValues): LogAttention | null {
    const hasDirectory = values.logDirectory.trim() !== "";
    return hasDirectory && isAllLoggingOff(values) ? { tone: "warning", label: "Silenced" } : null;
}

/**
 * The summary an operator actually wants above the fold: where output goes right now. It answers the
 * question the old single pill was standing in for - that pill reported whether a directory was set
 * while sitting on a card titled "Appliance log", so a shipped appliance logging to stdout at Info
 * read as "Disabled".
 *
 * Framework logging is deliberately absent: it selects which loggers are captured rather than where
 * output lands, so it is not a destination and its state belongs on its own card.
 */
export function describeDestinations(
    values: LogConfigurationFormValues,
    configuration: LogConfigurationResponse,
): LogDestination[] {
    const isOff = isAllLoggingOff(values);
    const directory = values.logDirectory.trim();
    const isAuditOn = configuration.auditLogs.level != null && configuration.auditLogs.level !== "Off";

    return [
        {
            key: "stdout",
            label: "Console",
            value: isOff ? "Silenced" : values.minLevel,
            tone: isOff ? "warning" : "positive",
            // The console has no setting of its own, but it is not "not configurable" either: the
            // appliance minimum level governs it, including silencing it at Off.
            hint: isOff
                ? "The appliance minimum level is Off, so nothing reaches stdout either."
                : `Quill writes to stdout at ${values.minLevel} and above, following the appliance minimum level.`,
        },
        {
            key: "file",
            label: "Log file",
            // A path here with the level Off would read as "still writing", so Off wins over the path.
            value: directory === "" ? "Not written" : isOff ? "Silenced" : directory,
            tone: directory === "" ? "muted" : isOff ? "warning" : "positive",
            hint:
                directory === ""
                    ? "No directory is set, so Quill writes no log file. Console output continues."
                    : isOff
                      ? `A directory is set, but the appliance level is Off so nothing is written to ${directory}.`
                      : `Quill writes quill.log in ${directory}.`,
        },
        {
            key: "audit",
            // No audit trail is a compliance gap rather than a preference, so it is the one sink that
            // reads as a warning when it is off instead of sharing the neutral "not configured" grey.
            label: "Audit",
            value: isAuditOn ? (configuration.auditLogs.level ?? "On") : "No trail",
            tone: isAuditOn ? "positive" : "warning",
            hint: isAuditOn
                ? "Authentication, and every change to access, data flow or spend, is recorded."
                : "Authentication, and every change to access, data flow or spend, goes unrecorded.",
        },
    ];
}

const FIELD_LABELS: Record<keyof LogConfigurationFormValues, string> = {
    minLevel: "Appliance level",
    logDirectory: "Log file directory",
    frameworkMinLevel: "Framework level",
    shouldPersist: "Keep after restart",
};

/**
 * One save can move four independent fields, so the action bar names them rather than only counting
 * them - "2 unsaved changes" alone still leaves the operator guessing which two.
 */
export function describeDirtyFields(dirtyFields: Partial<Record<keyof LogConfigurationFormValues, unknown>>): string[] {
    return (Object.keys(FIELD_LABELS) as (keyof LogConfigurationFormValues)[])
        .filter((field) => Boolean(dirtyFields[field]))
        .map((field) => FIELD_LABELS[field]);
}
