import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AgentSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { ApiState } from "@/components/data/api-state";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import { ParameterBindingFields } from "@/pages/apps/channels/parameter-binding-fields";
import {
    hasSameParameterNames,
    seedParameterRows,
    toParameterBindings,
} from "@/pages/apps/channels/parameter-bindings";
import {
    TELEGRAM_PARAMETER_SOURCES,
    TELEGRAM_SOURCE_VALUES,
    telegramParameterSourceHint,
} from "@/pages/apps/channels/telegram-parameter-sources";
import type { FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";

const parameterBindingSchema = z
    .object({
        name: z.string(),
        source: z.enum(TELEGRAM_SOURCE_VALUES),
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
        if (hasSameParameterNames(getValues("parameters") ?? [], seeded)) {
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
            await invalidateChannelQueries(queryClient, slug, "Telegram");
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
                        <ParameterBindingFields
                            control={form.control}
                            fields={parameterFields.fields}
                            rows={parameters}
                            sources={TELEGRAM_PARAMETER_SOURCES}
                            sourceHint={telegramParameterSourceHint}
                            description="Map each agent parameter to a constant value bound once for the whole channel, or to a field of the Telegram user sending each message."
                        />
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
