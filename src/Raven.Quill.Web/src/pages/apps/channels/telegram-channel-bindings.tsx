import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type {
    AgentSummaryResponse,
    ChannelSummaryResponse,
    TelegramParameterBinding,
} from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import { SectionEditActions } from "@/pages/apps/channels/channel-tab-actions";
import { EditableTabShell } from "@/pages/apps/channels/editable-tab-shell";
import {
    TELEGRAM_PARAMETER_SOURCES,
    telegramParameterSourceHint,
} from "@/pages/apps/channels/telegram-parameter-sources";

const parameterBindingSchema = z
    .object({
        name: z.string(),
        source: z.enum(["Constant", "UserId", "Username", "PhoneNumber"]),
        value: z.string().trim(),
    })
    .superRefine((parameter, ctx) => {
        if (parameter.source === "Constant" && parameter.value.trim().length === 0) {
            ctx.addIssue({ code: "custom", message: "Required", path: ["value"] });
        }
    });

const bindingsFormSchema = z.object({ parameters: z.array(parameterBindingSchema) });
type BindingsFormData = z.infer<typeof bindingsFormSchema>;

function toParameterBindings(parameters: BindingsFormData["parameters"]) {
    const bindings: Record<string, TelegramParameterBinding> = {};
    for (const { name, source, value } of parameters) {
        bindings[name] = { source, value: source === "Constant" ? value.trim() : null };
    }
    return bindings;
}

// The parameters to show are the ones the routed agent declares; each is pre-filled from the channel's
// existing binding when it has one. Falls back to the stored binding keys if the agent can't be resolved,
// so a channel never hides bindings it already has.
function seedRows(
    agent: AgentSummaryResponse | undefined,
    bindings: Record<string, TelegramParameterBinding> | null | undefined,
): BindingsFormData["parameters"] {
    const names = agent?.parameters?.length ? agent.parameters : Object.keys(bindings ?? {});
    return names.map((name) => {
        const binding = bindings?.[name];
        return {
            name,
            source: binding?.source ?? ("Constant" as const),
            value: binding?.source === "Constant" ? (binding.value ?? "") : "",
        };
    });
}

// The Telegram channel's parameter bindings, edited in the detail's "Parameters" tab: each agent
// parameter is filled from a constant value or a field of the Telegram user sending each message.
export function TelegramChannelBindings({
    slug,
    channel,
    agent,
}: {
    slug: string;
    channel: ChannelSummaryResponse;
    agent: AgentSummaryResponse | undefined;
}) {
    const queryClient = useQueryClient();
    const [isEditing, setIsEditing] = useState(false);

    const form = useForm<BindingsFormData>({
        mode: "onChange",
        resolver: zodResolver(bindingsFormSchema),
        defaultValues: { parameters: seedRows(agent, channel.telegram?.parameterBindings) },
    });

    const unsavedChanges = useFormUnsavedChanges(form);
    const parameterFields = useFieldArray({ control: form.control, name: "parameters" });
    const parameters = useWatch({ control: form.control, name: "parameters" }) ?? [];

    const updateMutation = useMutation({
        // Partial update: only the bindings change. Null telegram fields (botToken, messages) are left
        // untouched on the server.
        mutationFn: (values: BindingsFormData) =>
            api.services.channels.update(slug, channel.channelId, {
                displayName: null,
                allowedOrigins: null,
                enabled: null,
                telegram: { botToken: null, parameterBindings: toParameterBindings(values.parameters) },
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug);
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
                        {parameterFields.fields.map((field, index) => {
                            const hint = telegramParameterSourceHint(parameters[index]?.source);
                            return (
                                <div key={field.id} className="grid gap-2 rounded-md border bg-card p-4">
                                    <div className="grid gap-2 sm:grid-cols-2">
                                        <FormSelect
                                            control={form.control}
                                            name={`parameters.${index}.source`}
                                            label={field.name}
                                            options={TELEGRAM_PARAMETER_SOURCES}
                                            disabled={!isEditing}
                                        />
                                        {parameters[index]?.source === "Constant" && (
                                            <FormInput
                                                control={form.control}
                                                name={`parameters.${index}.value`}
                                                label="Value"
                                                placeholder="e.g. customers/1"
                                                disabled={!isEditing}
                                            />
                                        )}
                                    </div>
                                    {hint && <FieldDescription>{hint}</FieldDescription>}
                                </div>
                            );
                        })}
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
