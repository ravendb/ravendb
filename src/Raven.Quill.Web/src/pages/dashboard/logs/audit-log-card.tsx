import type { LogConfigurationResponse } from "@/api/generated/server-api";
import { EnabledStatus } from "@/components/data/status-indicator";
import { InlineCode } from "@/components/data/inline-code";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { describeRotation } from "./log-configuration-form-values";
import { LogFact } from "./log-fact";

// The audit log has no write path in the API at all, so this card is read-only by contract and
// not by choice.
export function AuditLogCard({ configuration }: { configuration: LogConfigurationResponse }) {
    const { auditLogs } = configuration;
    const isEnabled = auditLogs.level != null && auditLogs.level !== "Off";

    return (
        <Card>
            <CardHeader>
                <CardTitle>Audit log</CardTitle>
                <CardDescription>
                    Authentication events and every operation that changes access, data flow or spend.
                </CardDescription>
                <CardAction>
                    <EnabledStatus isEnabled={isEnabled} />
                </CardAction>
            </CardHeader>
            <CardContent className="space-y-4">
                <div className="grid gap-6 sm:grid-cols-2">
                    <LogFact label="Directory">
                        {auditLogs.path ?? <span className="text-muted-foreground">Not writing to a file</span>}
                    </LogFact>
                    <LogFact label="Rotation">{describeRotation(auditLogs)}</LogFact>
                </div>
                <p className="text-xs text-muted-foreground">
                    {isEnabled ? (
                        <>
                            The audit log is set in <InlineCode>quill.nlog.config</InlineCode> only and cannot be
                            changed here.
                        </>
                    ) : (
                        <>
                            There is no audit trail until this is switched on. Add{" "}
                            <InlineCode>QuillLoggingAudit</InlineCode> to the{" "}
                            <InlineCode>Raven_Default_Audit</InlineCode> rule in{" "}
                            <InlineCode>quill.nlog.config</InlineCode> and restart the appliance.
                        </>
                    )}
                </p>
            </CardContent>
        </Card>
    );
}
