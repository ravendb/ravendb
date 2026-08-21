import type { ReactNode } from "react";
import { Lock } from "lucide-react";
import type { LogConfigurationResponse } from "@/api/generated/server-api";
import { InlineCode } from "@/components/data/inline-code";
import { StatusIndicator, type StatusTone } from "@/components/data/status-indicator";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { LogSettingsGroup } from "./log-settings-group";
import { describeFilters, describeRotation } from "./log-configuration-form-values";

type ConfigFileRow = {
    log: string;
    setting: string;
    value: ReactNode;
    attention?: { tone: StatusTone; label: string };
};

/**
 * Everything a file edit and a restart are the only way to change, stated once.
 *
 * A table rather than a card of label/value pairs: a card is `bg-card` and raised above the page,
 * which is what marks the sections holding controls, and stacked pairs still read as a form whose
 * inputs went missing. Column headers name what each column is, and nobody expects to type into a
 * table cell. Follows the shape WRITER, ElevenLabs, Navattic and Cursor all use for the same job.
 */
export function ConfigFileSection({
    configuration,
    isFrameworkCaptured,
}: {
    configuration: LogConfigurationResponse;
    isFrameworkCaptured: boolean;
}) {
    const { auditLogs } = configuration;
    const isAuditEnabled = auditLogs.level != null && auditLogs.level !== "Off";
    const muted = (text: string) => <span className="text-muted-foreground">{text}</span>;

    /**
     * Ordered by consequence, not by subsystem: a missing audit trail is a compliance gap, framework
     * capture changes how much you can diagnose, and rotation and filters are housekeeping. The notes
     * under the table follow the same order, so the two cannot disagree about what matters most.
     */
    const rows: ConfigFileRow[] = [
        {
            log: "Audit log",
            setting: "Level",
            value: isAuditEnabled ? auditLogs.level : muted("Off"),
            // The one row on this page a compliance reviewer asks about.
            attention: isAuditEnabled ? undefined : { tone: "warning", label: "No audit trail" },
        },
        {
            log: "Audit log",
            setting: "Directory",
            value: auditLogs.path ?? muted("Not writing to a file"),
        },
        { log: "Audit log", setting: "Rotation", value: describeRotation(auditLogs) },
        // Framework capture only appears here when it is off: with capture on it is an editable level
        // and belongs in its own card instead.
        ...(isFrameworkCaptured
            ? []
            : [
                  {
                      log: "Framework",
                      setting: "Capture",
                      value: muted("Not captured"),
                      attention: { tone: "muted" as StatusTone, label: "No level to set" },
                  },
              ]),
        { log: "Appliance log", setting: "Rotation", value: describeRotation(configuration.logs) },
        { log: "Appliance log", setting: "Filters", value: describeFilters(configuration) },
    ];

    return (
        <LogSettingsGroup
            id="config-file-heading"
            title={
                <>
                    <Lock className="size-3.5 text-muted-foreground" aria-hidden="true" />
                    Set in <InlineCode>quill.nlog.config</InlineCode>
                </>
            }
            description="Changing any of these needs a file edit and an appliance restart."
            footer={
                /* Uncapped, so each instruction lands on one line. These are single imperative
                   sentences rather than prose, and the inline code chips make them wide enough that any
                   cap narrow enough to protect the reading measure would wrap them mid-clause. */
                (!isAuditEnabled || !isFrameworkCaptured) && (
                    <div className="mt-4 space-y-3 text-sm text-muted-foreground">
                        {!isAuditEnabled && (
                            <div className="space-y-1">
                                <p>
                                    <span className="font-medium text-foreground">To start an audit trail</span>, add{" "}
                                    <InlineCode>QuillLoggingAudit</InlineCode> to the{" "}
                                    <InlineCode>Raven_Default_Audit</InlineCode> rule, then restart the appliance.
                                </p>
                                {/* Worded exactly as the Audit chip's hint, so the same fact does not
                                    arrive in two different phrasings. */}
                                <p>
                                    Until then, authentication, and every change to access, data flow or spend, goes
                                    unrecorded.
                                </p>
                            </div>
                        )}
                        {/* The table row above already names what is not captured, so this only has to
                            say how to switch it on. */}
                        {!isFrameworkCaptured && (
                            <p>
                                <span className="font-medium text-foreground">To capture framework logs</span>, lower
                                the <InlineCode>finalMinLevel</InlineCode> of the{" "}
                                <InlineCode>Raven_Microsoft</InlineCode> and <InlineCode>Raven_System</InlineCode>{" "}
                                rules, then restart the appliance.
                            </p>
                        )}
                    </div>
                )
            }
        >
            {/* Log and Setting shrink to their content so Value takes the slack. */}
            <Table>
                <TableHeader>
                    <TableRow className="hover:bg-transparent">
                        {/* Roomier than the shared default: that padding is tuned for a long data
                                grid, and at six rows of reference it read as cramped. */}
                        <TableHead className="h-11 w-0 px-4 text-xs font-medium whitespace-nowrap text-muted-foreground">
                            Log
                        </TableHead>
                        <TableHead className="h-11 w-0 px-4 text-xs font-medium whitespace-nowrap text-muted-foreground">
                            Setting
                        </TableHead>
                        <TableHead className="h-11 px-4 text-xs font-medium text-muted-foreground">Value</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {rows.map((row) => (
                        <TableRow key={`${row.log}-${row.setting}`} className="hover:bg-transparent">
                            <TableCell className="px-4 py-4 whitespace-nowrap text-muted-foreground">
                                {row.log}
                            </TableCell>
                            <TableCell className="px-4 py-4 font-medium whitespace-nowrap">{row.setting}</TableCell>
                            <TableCell className="px-4 py-4">
                                <div className="flex flex-wrap items-center gap-2">
                                    <span className="break-all">{row.value}</span>
                                    {row.attention && (
                                        <StatusIndicator tone={row.attention.tone} label={row.attention.label} />
                                    )}
                                </div>
                            </TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </LogSettingsGroup>
    );
}
