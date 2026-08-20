import type { FormSelectOption } from "@/components/form/form-select";

// The generated `LogLevel` is a bare string: Sparrow.Logging.LogLevel is marked [Flags], which
// makes the schema exporter drop the enum list. So the levels are kept by hand here, in the
// server's order, and travel over the wire by name.
export const LOG_LEVELS = ["Trace", "Debug", "Info", "Warn", "Error", "Fatal", "Off"] as const;

export type QuillLogLevel = (typeof LOG_LEVELS)[number];

export const LOG_LEVEL_OPTIONS: readonly FormSelectOption<QuillLogLevel>[] = LOG_LEVELS.map((level) => ({
    value: level,
    label: level,
}));

// Off is deliberately missing: it is the state the server refuses to accept a microsoftLogs block
// in, so choosing it here would lock the control until someone edits quill.nlog.config and
// restarts. Fatal is the quietest level an operator can still undo from this page.
export const FRAMEWORK_LOG_LEVEL_OPTIONS = LOG_LEVEL_OPTIONS.filter((option) => option.value !== "Off");

export function parseLogLevel(value: string | undefined, fallback: QuillLogLevel): QuillLogLevel {
    return LOG_LEVELS.includes(value as QuillLogLevel) ? (value as QuillLogLevel) : fallback;
}
