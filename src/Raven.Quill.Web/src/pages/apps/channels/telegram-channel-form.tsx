import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AgentSummaryResponse, TelegramParameterBinding } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { ApiState } from "@/components/data/api-state";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import {
    TELEGRAM_PARAMETER_SOURCES,
    telegramParameterSourceHint,
} from "@/pages/apps/channels/telegram-parameter-sources";
import type { FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";

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

const telegramChannelSchema = z.object({
    agentId: z.string().min(1, "Select an agent to route conversations to"),
    displayName: z.string().trim(),
    botToken: z.string().trim().min(1, "Paste the bot token from @BotFather"),
    parameters: z.array(parameterBindingSchema),
});

type TelegramChannelFormData = z.infer<typeof telegramChannelSchema>;

function toParameterBindings(parameters: TelegramChannelFormData["parameters"]) {
    const bindings: Record<string, TelegramParameterBinding> = {};
    for (const { name, source, value } of parameters) {
        bindings[name] = { source, value: source === "Constant" ? value.trim() : null };
    }
    return bindings;
}

function seedParameterRows(agents: AgentSummaryResponse[], agentId: string): TelegramChannelFormData["parameters"] {
    const selected = agents.find((candidate) => candidate.agentId === agentId);
    return (selected?.parameters ?? []).map((name) => ({ name, source: "Constant" as const, value: "" }));
}

export function TelegramChannelForm({
    slug,
    agent,
    onCreated,
}: {
    slug: string;
    agent?: FixedAgent;
    onCreated: () => void;
}) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    if (!agentsQuery.data) {
        return (
            <div className="flex min-h-0 flex-1 flex-col">
                <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                    <ApiState
                        isLoading={agentsQuery.isPending}
                        isError={agentsQuery.isError}
                        errorTitle="Could not load agents"
                        onRetry={() => void agentsQuery.refetch()}
                        loadingLabel="Loading agents..."
                    >
                        {null}
                    </ApiState>
                </div>
            </div>
        );
    }

    return <LoadedTelegramChannelForm slug={slug} agent={agent} agents={agentsQuery.data} onCreated={onCreated} />;
}

function LoadedTelegramChannelForm({
    slug,
    agent,
    agents,
    onCreated,
}: {
    slug: string;
    agent?: FixedAgent;
    agents: AgentSummaryResponse[];
    onCreated: () => void;
}) {
    const queryClient = useQueryClient();

    const form = useForm<TelegramChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(telegramChannelSchema),
        defaultValues: {
            agentId: agent?.agentId ?? "",
            displayName: "",
            botToken: "",
            parameters: seedParameterRows(agents, agent?.agentId ?? ""),
        },
    });

    const unsavedChanges = useFormUnsavedChanges(form);

    const selectedAgentId = useWatch({ control: form.control, name: "agentId" });
    const parameterFields = useFieldArray({ control: form.control, name: "parameters" });
    const parameters = useWatch({ control: form.control, name: "parameters" }) ?? [];

    const { replace } = parameterFields;
    const { getValues } = form;
    useEffect(() => {
        const seeded = seedParameterRows(agents, selectedAgentId);
        const rows = getValues("parameters") ?? [];
        if (rows.length === seeded.length && seeded.every((row, index) => rows[index]?.name === row.name)) {
            return;
        }
        replace(seeded);
    }, [replace, getValues, selectedAgentId, agents]);

    const createMutation = useMutation({
        mutationFn: (values: TelegramChannelFormData) =>
            api.services.channels.create(slug, {
                type: "Telegram",
                agentId: values.agentId,
                allowedOrigins: null,
                displayName: values.displayName.trim() || null,
                telegram: {
                    botToken: values.botToken.trim(),
                    parameterBindings: values.parameters.length > 0 ? toParameterBindings(values.parameters) : null,
                },
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug);
            toast.success("Telegram channel created");
            onCreated();
        },
    });

    const agentOptions: FormSelectOption<string>[] = agents.map((option) => ({
        value: option.agentId,
        label: option.name,
    }));
    const hasAgentTarget = Boolean(agent?.agentId) || agents.length > 0;

    return (
        <form
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={withNestedSubmit(form.handleSubmit((values) => createMutation.mutate(values)))}
        >
            <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                {!hasAgentTarget ? (
                    <Alert>
                        Create an agent first — a channel routes conversations to one of the app&apos;s agents.
                    </Alert>
                ) : (
                    <>
                        {!agent && (
                            <FormSelect
                                control={form.control}
                                name="agentId"
                                label="Agent"
                                placeholder="Select an agent"
                                options={agentOptions}
                                description="Messages to this bot are answered by this agent."
                            />
                        )}
                        <FormInput
                            control={form.control}
                            name="botToken"
                            type="password"
                            label="Bot token"
                            placeholder="123456789:AA..."
                            description="Create a bot with @BotFather and paste its token. It is validated with Telegram and never shown again."
                        />
                        <FormInput
                            control={form.control}
                            name="displayName"
                            label="Channel name"
                            placeholder="Defaults to the bot's username"
                            description="Shown in the channels list. Optional."
                        />
                        {parameterFields.fields.length > 0 && (
                            <div className="flex flex-col gap-3">
                                <div className="space-y-0.5">
                                    <h3 className="text-sm font-medium">Parameters</h3>
                                    <p className="text-xs text-muted-foreground">
                                        Map each agent parameter to a constant value bound once for the whole channel,
                                        or to a field of the Telegram user sending each message.
                                    </p>
                                </div>
                                {parameterFields.fields.map((field, index) => {
                                    const hint = telegramParameterSourceHint(parameters[index]?.source);
                                    return (
                                        <div key={field.id} className="grid gap-2">
                                            <div className="grid gap-2 sm:grid-cols-2">
                                                <FormSelect
                                                    control={form.control}
                                                    name={`parameters.${index}.source`}
                                                    label={field.name}
                                                    options={TELEGRAM_PARAMETER_SOURCES}
                                                />
                                                {parameters[index]?.source === "Constant" && (
                                                    <FormInput
                                                        control={form.control}
                                                        name={`parameters.${index}.value`}
                                                        label="Value"
                                                        placeholder="e.g. customers/1"
                                                    />
                                                )}
                                            </div>
                                            {hint && <FieldDescription>{hint}</FieldDescription>}
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </>
                )}

                {createMutation.isError && (
                    <Alert variant="destructive">
                        {createMutation.error instanceof Error
                            ? createMutation.error.message.split("\n")[0]
                            : "Could not create channel."}
                    </Alert>
                )}
            </div>

            <SheetFooter className="flex-row justify-end border-t">
                <SheetClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </SheetClose>
                <Button type="submit" disabled={createMutation.isPending || !hasAgentTarget}>
                    {createMutation.isPending && <Spinner />}
                    Connect bot
                </Button>
            </SheetFooter>
        </form>
    );
}
