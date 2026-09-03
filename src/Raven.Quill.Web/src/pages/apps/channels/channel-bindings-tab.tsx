import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type {
    AgentSummaryResponse,
    ChannelParameterBinding,
    ChannelSummaryResponse,
    UpdateChannelRequest,
} from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import type { FormSelectOption } from "@/components/form/form-select";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import { SectionEditActions } from "@/pages/apps/channels/channel-tab-actions";
import { EditableTabShell } from "@/pages/apps/channels/editable-tab-shell";
import { ParameterBindingRow } from "@/pages/apps/channels/parameter-binding-fields";
import {
    parameterBindingsFormSchema,
    seedEditRows,
    toParameterBindings,
    type ParameterBindingsFormData,
    type ParameterSource,
} from "@/pages/apps/channels/parameter-bindings";

export function ChannelBindingsTab({
    slug,
    channel,
    agent,
    bindings,
    sourceValues,
    sources,
    sourceHint,
    buildUpdateRequest,
}: {
    slug: string;
    channel: ChannelSummaryResponse;
    agent: AgentSummaryResponse | undefined;
    bindings: Record<string, ChannelParameterBinding> | null | undefined;
    sourceValues: readonly [ParameterSource, ...ParameterSource[]];
    sources: readonly FormSelectOption<ParameterSource>[];
    sourceHint: (source: ParameterSource | undefined) => string | undefined;
    buildUpdateRequest: (parameterBindings: Record<string, ChannelParameterBinding>) => UpdateChannelRequest;
}) {
    const queryClient = useQueryClient();
    const [isEditing, setIsEditing] = useState(false);

    const form = useForm<ParameterBindingsFormData>({
        mode: "onChange",
        resolver: zodResolver(parameterBindingsFormSchema(sourceValues)),
        defaultValues: { parameters: seedEditRows(agent, bindings, sourceValues) },
    });

    const unsavedChanges = useFormUnsavedChanges(form);
    const parameterFields = useFieldArray({ control: form.control, name: "parameters" });
    const parameters = useWatch({ control: form.control, name: "parameters" }) ?? [];

    const updateMutation = useMutation({
        mutationFn: (values: ParameterBindingsFormData) =>
            api.services.channels.update(
                slug,
                channel.channelId,
                buildUpdateRequest(toParameterBindings(values.parameters)),
            ),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug, channel.type);
            toast.success("Parameter bindings saved");
            setIsEditing(false);
        },
    });

    const hasParameters = parameterFields.fields.length > 0;

    const onCancel = () => {
        form.reset();
        setIsEditing(false);
    };

    return (
        <form
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={form.handleSubmit((values) => updateMutation.mutate(values))}
        >
            <EditableTabShell
                title="Parameter bindings"
                description="How each agent parameter is filled for conversations on this channel."
                actions={
                    hasParameters && (
                        <SectionEditActions
                            isEditing={isEditing}
                            isSaving={updateMutation.isPending}
                            onEdit={() => setIsEditing(true)}
                            onCancel={onCancel}
                        />
                    )
                }
            >
                {hasParameters ? (
                    <div className="flex flex-col gap-4">
                        {parameterFields.fields.map((field, index) => (
                            <ParameterBindingRow
                                key={field.id}
                                control={form.control}
                                index={index}
                                label={field.name}
                                source={parameters[index]?.source}
                                sources={sources}
                                sourceHint={sourceHint}
                                disabled={!isEditing}
                                className="rounded-md border bg-card p-4"
                            />
                        ))}
                    </div>
                ) : (
                    <Alert>The agent declares no parameters, so this channel binds nothing.</Alert>
                )}

                {updateMutation.isError && (
                    <Alert variant="destructive" className="mt-3">
                        {updateMutation.error instanceof Error
                            ? updateMutation.error.message.split("\n")[0]
                            : "Could not save the parameter bindings."}
                    </Alert>
                )}
            </EditableTabShell>
        </form>
    );
}
