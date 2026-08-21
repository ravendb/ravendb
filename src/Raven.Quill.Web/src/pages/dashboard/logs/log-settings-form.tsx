import { useState } from "react";
import { CircleAlert, TriangleAlert, Undo2 } from "lucide-react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch } from "react-hook-form";
import type { LogConfigurationResponse } from "@/api/generated/server-api";
import { InlineCode } from "@/components/data/inline-code";
import { StatusIndicator } from "@/components/data/status-indicator";
import { FormInput } from "@/components/form/form-input";
import { FormToggleGroup, type FormToggleGroupOption } from "@/components/form/form-toggle-group";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { FieldGroup } from "@/components/shadcn/ui/field";
import { InputGroupAddon } from "@/components/shadcn/ui/input-group";
import { ConfigFileSection } from "./config-file-section";
import { LogSettingNote, LogSettingRow, LogSettingValue } from "./log-setting-row";
import { LogSettingsGroup } from "./log-settings-group";
import {
    isFrameworkCaptureOn,
    logConfigurationFormSchema,
    LOG_LEVELS,
    toFormValues,
} from "./log-configuration-form-values";
import { LogSettingsToolbar } from "./log-settings-toolbar";
import {
    describeDestinations,
    describeDirtyFields,
    describeDrift,
    describeFileAttention,
    isAllLoggingOff,
    type LogDrift,
} from "./log-settings-summary";
import { useLogConfigurationSave } from "./use-log-configuration-save";

const LEVEL_OPTIONS: readonly FormToggleGroupOption[] = LOG_LEVELS.map((level) => ({ value: level, label: level }));

// Off is deliberately missing: it is the state the server refuses to accept a microsoftLogs block
// in, so choosing it here would lock the control until someone edits quill.nlog.config and
// restarts. Fatal is the quietest level an operator can still undo from this page.
const FRAMEWORK_LEVEL_OPTIONS = LEVEL_OPTIONS.filter((option) => option.value !== "Off");

// One source for each row's wording, so the read-only and editable renderings cannot drift apart.
//
// Every description says what the setting does before what it forbids, in the active voice, one fact
// per sentence. They used to open passively ("Messages below this level are dropped") and stack three
// unrelated facts into a single sentence, which is what made them read as reference rather than help.
const APPLIANCE_LEVEL_FIELD = {
    label: "Appliance minimum level",
    description: "Keeps this level and anything more severe. Quieter messages never reach the console or the file.",
};

// Naming the volume replaces "use an absolute path, so the file survives a recreate", which promised
// something an absolute path does not deliver - /tmp/logs is absolute and still dies with the
// container. Saying "under /var/lib/quill" also makes "absolute" redundant, since any path under it
// already is one.
//
// What was dropped: that changing the path leaves existing files alone. True, but reassurance about
// something that does not go wrong, and the confirm dialog covers it where it matters.
const DIRECTORY_FIELD = {
    label: "Log file directory",
    description: (
        <>
            Where Quill writes <InlineCode>quill.log</InlineCode>. Keep it under <InlineCode>/var/lib/quill</InlineCode>
            ; anywhere else is lost on recreate. Leave empty to stop writing a file.
        </>
    ),
};

// No note about Off being absent from the ladder: nobody misses an option they never saw, and why it
// is withheld is a fact about the server contract rather than help with the setting. The reasoning
// lives on FRAMEWORK_LEVEL_OPTIONS, where the next person to change that list will read it.
const FRAMEWORK_LEVEL_FIELD = {
    label: "Framework minimum level",
    description: "Keeps this level and anything more severe, before any other rule applies.",
};

export function LogSettingsForm({ configuration }: { configuration: LogConfigurationResponse }) {
    const [isEditing, setIsEditing] = useState(false);
    const form = useForm({
        resolver: zodResolver(logConfigurationFormSchema),
        defaultValues: toFormValues(configuration),
    });
    const unsavedChanges = useFormUnsavedChanges(form);
    const {
        liveDirectory,
        partialSaveMessage,
        saveMutation,
        submit,
        valuesAwaitingConfirmation,
        setValuesAwaitingConfirmation,
    } = useLogConfigurationSave(configuration, form, unsavedChanges, () => setIsEditing(false));

    const isFrameworkCaptured = isFrameworkCaptureOn(configuration);
    const draft = useWatch({ control: form.control });
    // useWatch is untyped-partial on the first render, so fall back to the seeded values.
    const values = { ...toFormValues(configuration), ...draft };

    const fileAttention = describeFileAttention(values);
    const destinations = describeDestinations(values, configuration);
    const isSwitchingFileOff = values.logDirectory.trim() === "" && liveDirectory !== "";
    // Both loggers report a booted level alongside the running one, so both surface it the same way.
    const applianceDrift = describeDrift(configuration.logs);
    const frameworkDrift = describeDrift(configuration.microsoftLogs);

    /** Only offered out of edit mode: while editing, the ladder already exposes that level. */
    const renderDriftNote = (drift: LogDrift | null, field: "minLevel" | "frameworkMinLevel") =>
        drift &&
        !isEditing && (
            <LogSettingNote>
                <span className="text-muted-foreground">Booted at {drift.startupLevel}</span>
                <Button
                    type="button"
                    variant="link"
                    className="h-auto gap-1 p-0 text-xs no-underline hover:no-underline hover:opacity-70"
                    aria-label={`Revert to ${drift.startupLevel}`}
                    onClick={() => {
                        setIsEditing(true);
                        form.setValue(field, drift.startupLevel, { shouldDirty: true });
                    }}
                >
                    <Undo2 className="size-3" aria-hidden="true" />
                    Revert
                </Button>
            </LogSettingNote>
        );

    return (
        <form onSubmit={submit} noValidate className="space-y-6">
            <LogSettingsToolbar
                control={form.control}
                canPersist={configuration.canPersist}
                destinations={destinations}
                dirtyCount={describeDirtyFields(form.formState.dirtyFields).length}
                isEditing={isEditing}
                isSaving={saveMutation.isPending}
                onDiscard={() => {
                    form.reset(toFormValues(configuration));
                    setIsEditing(false);
                }}
                onEdit={() => setIsEditing(true)}
            />

            {partialSaveMessage && (
                <Alert variant="warning" className="border-warning/40 bg-warning/5">
                    <TriangleAlert aria-hidden="true" />
                    <AlertTitle>Applied but not saved</AlertTitle>
                    <AlertDescription>{partialSaveMessage}</AlertDescription>
                </Alert>
            )}

            {saveMutation.isError && !partialSaveMessage && (
                <Alert variant="destructive" className="border-destructive/40 bg-destructive/5">
                    <CircleAlert aria-hidden="true" />
                    <AlertTitle>Could not save the log settings</AlertTitle>
                    <AlertDescription>
                        {saveMutation.error instanceof Error
                            ? saveMutation.error.message
                            : "The appliance refused the change."}
                    </AlertDescription>
                </Alert>
            )}

            <LogSettingsGroup
                id="appliance-log-heading"
                title="Appliance log"
                description="Everything Quill itself writes."
            >
                {/* Rows keep the description in a column of its own, so no viewport width can stretch
                    it into a 200-character line. FieldGroup owns the container query the responsive
                    orientation breaks on. */}
                <FieldGroup className="settings-rows divide-y">
                    {/* A field and the warning it triggers are one row, not two. As siblings the
                        alert picked up a divider above and below it and read as an unrelated band
                        wedged between two settings. */}
                    <div className="space-y-3">
                        {/* Levels are ordinal, so the ladder shows where the current one sits on the
                            scale instead of hiding the ordering behind a closed dropdown. Out of edit
                            mode the scale is noise - only the current value matters. The label names
                            the log it belongs to; two controls both called "Minimum level" announced
                            identically to a screen reader. */}
                        {isEditing ? (
                            <FormToggleGroup
                                control={form.control}
                                name="minLevel"
                                label={APPLIANCE_LEVEL_FIELD.label}
                                description={APPLIANCE_LEVEL_FIELD.description}
                                options={LEVEL_OPTIONS}
                                canDeselect={false}
                                spacing={0}
                                orientation="responsive"
                            />
                        ) : (
                            <LogSettingRow
                                {...APPLIANCE_LEVEL_FIELD}
                                /* Drift belongs to this one setting, so it sits under its own row rather
                               than in the page toolbar, where it was orphaned from the value it refers
                               to, or in the value column, where a 130px action next to a 60px value
                               inverted their weight.

                               Being offered at all is the whole status, so there is no badge - the way
                               Teachable and Optimal Workshop show "reset" only on a field that has moved
                               off its default. Reverting stages the boot level and opens the controls
                               rather than saving outright, since it is still a change to the running
                               appliance. Hidden while editing, where the ladder already offers it. */
                                note={renderDriftNote(applianceDrift, "minLevel")}
                            >
                                <LogSettingValue>{values.minLevel}</LogSettingValue>
                            </LogSettingRow>
                        )}

                        {isAllLoggingOff(values) && (
                            <Alert variant="warning" className="max-w-prose border-warning/40 bg-warning/5">
                                <TriangleAlert aria-hidden="true" />
                                <AlertTitle>Off silences the console too</AlertTitle>
                                <AlertDescription>
                                    Nothing is written anywhere, including stdout, so diagnosing the appliance means
                                    turning this back on first. You can, from here.
                                </AlertDescription>
                            </Alert>
                        )}
                    </div>

                    <div className="space-y-3">
                        {isEditing ? (
                            <FormInput
                                control={form.control}
                                name="logDirectory"
                                label={DIRECTORY_FIELD.label}
                                description={DIRECTORY_FIELD.description}
                                placeholder="e.g. /var/lib/quill/logs"
                                autoComplete="off"
                                spellCheck={false}
                                orientation="responsive"
                                addons={
                                    fileAttention && (
                                        <InputGroupAddon align="inline-end">
                                            <StatusIndicator tone={fileAttention.tone} label={fileAttention.label} />
                                        </InputGroupAddon>
                                    )
                                }
                            />
                        ) : (
                            <LogSettingRow {...DIRECTORY_FIELD}>
                                <div className="flex min-w-0 items-center gap-2">
                                    <LogSettingValue>
                                        {values.logDirectory.trim() === "" ? (
                                            <span className="text-muted-foreground">No file written</span>
                                        ) : (
                                            values.logDirectory
                                        )}
                                    </LogSettingValue>
                                    {fileAttention && (
                                        <StatusIndicator tone={fileAttention.tone} label={fileAttention.label} />
                                    )}
                                </div>
                            </LogSettingRow>
                        )}

                        {isSwitchingFileOff && (
                            <Alert variant="warning" className="max-w-prose border-warning/40 bg-warning/5">
                                <TriangleAlert aria-hidden="true" />
                                <AlertTitle>Saving now switches the log file off</AlertTitle>
                                <AlertDescription>
                                    The files already in {liveDirectory} are left alone, and console logging continues.
                                </AlertDescription>
                            </Alert>
                        )}
                    </div>

                    {/* Stated explicitly because the old page implied the opposite: with no directory
                            set it reported "Disabled" while the appliance was still logging to stdout.
                            The copy carries the state, so the row needs no badge of its own. */}
                    <LogSettingRow
                        label="Console output"
                        // Not "always on and not configurable" - the appliance minimum level silences
                        // stdout when set to Off, so that read as a flat contradiction. What is true is
                        // that the console has no setting of its own: it follows that one level, and
                        // cannot be pointed at a different one or switched off independently.
                        description={
                            isAllLoggingOff(values)
                                ? "Nothing reaches stdout while the appliance minimum level is Off. That level is its only control."
                                : "Everything Quill keeps also goes to stdout. The appliance minimum level is its only control."
                        }
                    />
                </FieldGroup>
            </LogSettingsGroup>

            {/* Only a group of its own while there is something to set. With capture off there is no
                level, so its state moves into the read-only table rather than sitting in a panel that
                looks like settings whose inputs went missing. */}
            {isFrameworkCaptured && (
                <LogSettingsGroup
                    id="framework-logging-heading"
                    title="Framework logging"
                    // Parallel with "Everything Quill itself writes", so the pair reads as a
                    // distinction rather than as two unrelated labels.
                    description={
                        <>
                            What .NET&apos;s own <InlineCode>Microsoft.*</InlineCode> and{" "}
                            <InlineCode>System.*</InlineCode> loggers write.
                        </>
                    }
                >
                    <FieldGroup className="settings-rows">
                        {isEditing ? (
                            <FormToggleGroup
                                control={form.control}
                                name="frameworkMinLevel"
                                label={FRAMEWORK_LEVEL_FIELD.label}
                                description={FRAMEWORK_LEVEL_FIELD.description}
                                options={FRAMEWORK_LEVEL_OPTIONS}
                                canDeselect={false}
                                spacing={0}
                                orientation="responsive"
                            />
                        ) : (
                            <LogSettingRow
                                {...FRAMEWORK_LEVEL_FIELD}
                                note={renderDriftNote(frameworkDrift, "frameworkMinLevel")}
                            >
                                <LogSettingValue>{values.frameworkMinLevel}</LogSettingValue>
                            </LogSettingRow>
                        )}
                    </FieldGroup>
                </LogSettingsGroup>
            )}

            <ConfigFileSection configuration={configuration} isFrameworkCaptured={isFrameworkCaptured} />

            <ConfirmDialog
                open={valuesAwaitingConfirmation !== null}
                onOpenChange={(open) => {
                    if (!open) {
                        setValuesAwaitingConfirmation(null);
                    }
                }}
                variant="warning"
                title="Switch the log file off?"
                description={`Quill stops writing quill.log. Files already in ${liveDirectory} stay where they are, and console logging carries on.`}
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
