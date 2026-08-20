import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch } from "react-hook-form";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { LogConfigurationResponse } from "@/api/generated/server-api";
import { isApiError } from "@/api/http-client";
import { InlineCode } from "@/components/data/inline-code";
import { EnabledStatus, StatusIndicator } from "@/components/data/status-indicator";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormSwitch } from "@/components/form/form-switch";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    isFrameworkCaptureOn,
    logConfigurationFormSchema,
    toFormValues,
    toUpdateRequest,
    type LogConfigurationFormValues,
} from "./log-configuration-form-values";
import { FRAMEWORK_LOG_LEVEL_OPTIONS, LOG_LEVEL_OPTIONS } from "./log-levels";
import { describeRotation } from "./log-configuration-form-values";
import { LogFact } from "./log-fact";

// A settings change is only trustworthy once the appliance has echoed it back: ResolvePath turns a
// relative directory absolute, so the response carries the path that is actually in use.
async function refetchConfiguration(queryClient: ReturnType<typeof useQueryClient>) {
    try {
        return await queryClient.fetchQuery({ ...api.queries.settings.logConfiguration(), staleTime: 0 });
    } catch {
        return undefined;
    }
}

export function LogConfigurationForm({ configuration }: { configuration: LogConfigurationResponse }) {
    const queryClient = useQueryClient();
    const [partialSaveMessage, setPartialSaveMessage] = useState<string | null>(null);
    const [valuesAwaitingConfirmation, setValuesAwaitingConfirmation] = useState<LogConfigurationFormValues | null>(
        null,
    );

    const form = useForm<LogConfigurationFormValues>({
        resolver: zodResolver(logConfigurationFormSchema),
        defaultValues: toFormValues(configuration),
    });
    const unsavedChanges = useFormUnsavedChanges(form);

    const isFrameworkCaptured = isFrameworkCaptureOn(configuration);
    const liveDirectory = configuration.logs.path ?? "";
    const draftDirectory = useWatch({ control: form.control, name: "logDirectory" });
    const draftMinLevel = useWatch({ control: form.control, name: "minLevel" });
    const isSwitchingFileOff = draftDirectory.trim() === "" && liveDirectory !== "";

    const saveMutation = useMutation({
        mutationFn: (values: LogConfigurationFormValues) =>
            api.services.settings.updateLogConfiguration(toUpdateRequest(values, configuration)),
        onSuccess: async (_result, values) => {
            setPartialSaveMessage(null);
            await reseed();
            toast.success(
                configuration.canPersist && values.shouldPersist ? "Log settings saved" : "Log settings applied",
            );
        },
        onError: async (error) => {
            // 500 is a partial success: the running appliance did change and only writing the file
            // failed. Leaving the form dirty would claim the opposite.
            if (isApiError(error) && error.status === 500) {
                setPartialSaveMessage(error.message);
                await reseed();
                toast.warning("Log settings applied but not saved", {
                    description: "They will be lost when the appliance restarts.",
                });
            }
            // Everything else changed nothing, so the rejected value stays in the field to be
            // corrected and the alert below carries the server's reason.
        },
    });

    async function reseed() {
        const fresh = await refetchConfiguration(queryClient);
        if (fresh) {
            form.reset(toFormValues(fresh));
        }
        unsavedChanges.markSaved();
    }

    const submit = form.handleSubmit((values) => {
        if (values.logDirectory.trim() === "" && liveDirectory !== "") {
            setValuesAwaitingConfirmation(values);
            return;
        }
        saveMutation.mutate(values);
    });

    return (
        <form className="space-y-6" onSubmit={submit} noValidate>
            <Card>
                <CardHeader>
                    <CardTitle>Appliance log</CardTitle>
                    <CardDescription>
                        Everything the appliance writes goes to stdout. Add a directory to also write{" "}
                        <InlineCode>quill.log</InlineCode> there.
                    </CardDescription>
                    <CardAction>
                        <EnabledStatus isEnabled={liveDirectory !== ""} />
                    </CardAction>
                </CardHeader>
                <CardContent className="space-y-4">
                    <FormSelect
                        control={form.control}
                        name="minLevel"
                        label="Minimum level"
                        options={LOG_LEVEL_OPTIONS}
                        className="max-w-xs"
                        description="Messages below this level are dropped, on stdout and in the file."
                    />
                    {draftMinLevel === "Off" && (
                        <Alert variant="warning">
                            <AlertTitle>Off stops all appliance logging</AlertTitle>
                            <AlertDescription>
                                Nothing is written, including stdout. You can turn it back on from here.
                            </AlertDescription>
                        </Alert>
                    )}

                    <FormInput
                        control={form.control}
                        name="logDirectory"
                        label="Log file directory"
                        placeholder="e.g. /var/lib/quill/logs"
                        autoComplete="off"
                        spellCheck={false}
                        className="max-w-xl"
                        description={
                            <>
                                An absolute path on the appliance, such as <InlineCode>/var/lib/quill/logs</InlineCode>,
                                so the file survives a recreate. Leave empty to switch the log file off.{" "}
                                <InlineCode>quill.log</InlineCode> is written in this directory, and files already
                                written are not moved when it changes.
                            </>
                        }
                    />
                    {isSwitchingFileOff && (
                        <Alert variant="warning">
                            <AlertTitle>Saving now switches the log file off</AlertTitle>
                            <AlertDescription>
                                The files already in {liveDirectory} are left alone, and stdout logging continues.
                            </AlertDescription>
                        </Alert>
                    )}

                    <div className="grid gap-6 sm:grid-cols-3">
                        <LogFact label="At startup">{configuration.logs.minLevel ?? "Not reported"}</LogFact>
                        <LogFact label="Rotation">{describeRotation(configuration.logs)}</LogFact>
                        <LogFact label="Filters">{describeFilters(configuration)}</LogFact>
                    </div>
                    <p className="text-xs text-muted-foreground">
                        Rotation and filters are set in <InlineCode>quill.nlog.config</InlineCode> and cannot be changed
                        here.
                    </p>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <CardTitle>Framework logging</CardTitle>
                    <CardDescription>
                        Levels for the <InlineCode>Microsoft.*</InlineCode> and <InlineCode>System.*</InlineCode>{" "}
                        loggers.
                    </CardDescription>
                    <CardAction>
                        {isFrameworkCaptured ? (
                            <EnabledStatus isEnabled />
                        ) : (
                            <StatusIndicator tone="muted" label="Not captured" />
                        )}
                    </CardAction>
                </CardHeader>
                <CardContent className="space-y-4">
                    {isFrameworkCaptured ? (
                        <>
                            <FormSelect
                                control={form.control}
                                name="frameworkMinLevel"
                                label="Minimum level"
                                options={FRAMEWORK_LOG_LEVEL_OPTIONS}
                                className="max-w-xs"
                                description="Messages below this level are dropped before any other rule sees them."
                            />
                            <p className="text-xs text-muted-foreground">
                                Off is not offered here: it stops capture, and switching capture back on needs a{" "}
                                <InlineCode>quill.nlog.config</InlineCode> edit and a restart. Fatal is the quietest
                                level you can undo.
                            </p>
                        </>
                    ) : (
                        <p className="text-sm text-muted-foreground">
                            Microsoft and System logging is not captured, so its level cannot be changed here. Lower the{" "}
                            <InlineCode>finalMinLevel</InlineCode> of the <InlineCode>Raven_Microsoft</InlineCode> and{" "}
                            <InlineCode>Raven_System</InlineCode> rules in <InlineCode>quill.nlog.config</InlineCode>{" "}
                            and restart the appliance to switch it on.
                        </p>
                    )}
                    <div className="grid gap-6 sm:grid-cols-3">
                        <LogFact label="At startup">{configuration.microsoftLogs.minLevel ?? "Not reported"}</LogFact>
                    </div>
                </CardContent>
            </Card>

            {partialSaveMessage && (
                <Alert variant="warning">
                    <AlertTitle>Applied but not saved</AlertTitle>
                    <AlertDescription>{partialSaveMessage}</AlertDescription>
                </Alert>
            )}

            {saveMutation.isError && !partialSaveMessage && (
                <Alert variant="destructive">
                    {saveMutation.error instanceof Error
                        ? saveMutation.error.message
                        : "Could not save the log settings."}
                </Alert>
            )}

            <div className="flex flex-wrap items-center justify-between gap-4 border-t pt-4">
                <div className="space-y-1">
                    <FormSwitch
                        control={form.control}
                        name="shouldPersist"
                        label="Save to quill.nlog.config"
                        disabled={!configuration.canPersist}
                    />
                    <p className="text-xs text-muted-foreground">
                        {configuration.canPersist
                            ? "Also write these settings to the file so they survive a restart. With this off, the change applies to the running appliance only."
                            : "No writable quill.nlog.config is configured, so changes apply to the running appliance only and are lost on restart."}
                    </p>
                </div>
                <Button type="submit" disabled={!form.formState.isDirty || saveMutation.isPending}>
                    {saveMutation.isPending && <Spinner />}
                    Save changes
                </Button>
            </div>

            <ConfirmDialog
                open={valuesAwaitingConfirmation !== null}
                onOpenChange={(open) => {
                    if (!open) {
                        setValuesAwaitingConfirmation(null);
                    }
                }}
                variant="warning"
                title="Switch the log file off?"
                description={`The log file directory is empty, so the appliance stops writing quill.log. The files already in ${liveDirectory} are left alone, and stdout logging continues.`}
                confirmLabel="Switch it off"
                cancelLabel="Keep the file"
                onConfirm={() => {
                    if (valuesAwaitingConfirmation) {
                        saveMutation.mutate(valuesAwaitingConfirmation);
                    }
                }}
            />
        </form>
    );
}

// LogFilter generates as an opaque record, so only the count and the default action are readable.
function describeFilters(configuration: LogConfigurationResponse): string {
    const count = configuration.logs.currentFilters?.length ?? 0;
    if (count === 0) {
        return "None";
    }
    const defaultAction = configuration.logs.currentLogFilterDefaultAction;
    return defaultAction ? `${count} active (default action: ${defaultAction})` : `${count} active`;
}
