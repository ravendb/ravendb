import { useEffect, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { ChevronDown } from "lucide-react";
import { api } from "@/api/api";
import type { AgentSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { ApiState } from "@/components/data/api-state";
import { Heading, Text } from "@/components/typography";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { ParameterBindingFields } from "@/pages/apps/channels/parameter-binding-fields";
import {
    hasSameParameterNames,
    seedParameterRows,
    toParameterBindings,
} from "@/pages/apps/channels/parameter-bindings";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import { DISCORD_DEVELOPER_PORTAL_URL } from "@/pages/apps/channels/discord-app-setup";
import {
    DISCORD_PARAMETER_SOURCES,
    DISCORD_SOURCE_VALUES,
    discordParameterSourceHint,
} from "@/pages/apps/channels/discord-parameter-sources";
import { DiscordStatusPanel } from "@/pages/apps/channels/discord-status-panel";
import type { FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";

const parameterBindingSchema = z
    .object({
        name: z.string(),
        source: z.enum(DISCORD_SOURCE_VALUES),
        value: z.string().trim(),
    })
    .superRefine((parameter, ctx) => {
        if (parameter.source === "Constant" && parameter.value.trim().length === 0) {
            ctx.addIssue({ code: "custom", message: "Required", path: ["value"] });
        }
    });

const discordChannelSchema = z.object({
    agentId: z.string().min(1, "Select an agent to route conversations to"),
    displayName: z.string().trim(),
    botToken: z
        .string()
        .trim()
        .min(1, "Paste the bot token from the app's Bot page")
        .refine((token) => /^\S+$/.test(token), "A bot token contains no spaces"),
    parameters: z.array(parameterBindingSchema),
});

type DiscordChannelFormData = z.infer<typeof discordChannelSchema>;

export function DiscordChannelForm({ slug, agent, onDone }: { slug: string; agent?: FixedAgent; onDone: () => void }) {
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

    return <LoadedDiscordChannelForm slug={slug} agent={agent} agents={agentsQuery.data} onDone={onDone} />;
}

function LoadedDiscordChannelForm({
    slug,
    agent,
    agents,
    onDone,
}: {
    slug: string;
    agent?: FixedAgent;
    agents: AgentSummaryResponse[];
    onDone: () => void;
}) {
    const queryClient = useQueryClient();
    const [isSetupOpen, setIsSetupOpen] = useState(false);

    const form = useForm<DiscordChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(discordChannelSchema),
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
        mutationFn: (values: DiscordChannelFormData) =>
            api.services.channels.create(slug, {
                type: "Discord",
                agentId: values.agentId,
                allowedOrigins: null,
                displayName: values.displayName.trim() || null,
                discord: {
                    botToken: values.botToken.trim(),
                    parameterBindings: values.parameters.length > 0 ? toParameterBindings(values.parameters) : null,
                },
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug, "Discord");
            toast.success("Discord channel created");
        },
    });

    const createdChannelId = createMutation.data?.channelId;
    if (createdChannelId) {
        return (
            <div className="flex min-h-0 flex-1 flex-col">
                <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                    <DiscordStatusPanel slug={slug} channelId={createdChannelId} />
                </div>
                <SheetFooter className="flex-row justify-end border-t">
                    <Button type="button" onClick={onDone}>
                        Done
                    </Button>
                </SheetFooter>
            </div>
        );
    }

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
                        <Collapsible open={isSetupOpen} onOpenChange={setIsSetupOpen} className="grid gap-3">
                            <CollapsibleTrigger className="group flex w-full items-start justify-between gap-3 text-left">
                                <div>
                                    <Heading as="h3" variant="label">
                                        No Discord app yet?
                                    </Heading>
                                    <Text variant="caption" className="mt-1">
                                        Create one in the Developer Portal, then paste its bot token below.
                                    </Text>
                                </div>
                                <ChevronDown
                                    className="mt-0.5 size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                                    aria-hidden="true"
                                />
                            </CollapsibleTrigger>
                            <CollapsibleContent className="grid gap-2">
                                <ol className="list-decimal space-y-1.5 ps-5 text-xs text-muted-foreground">
                                    <li>
                                        At{" "}
                                        <a
                                            href={DISCORD_DEVELOPER_PORTAL_URL}
                                            target="_blank"
                                            rel="noreferrer"
                                            className="underline"
                                        >
                                            discord.com/developers/applications
                                        </a>
                                        , choose <span className="font-medium">New Application</span> and name it.
                                    </li>
                                    <li>
                                        Open the <span className="font-medium">Bot</span> page, choose{" "}
                                        <span className="font-medium">Reset Token</span> and copy the token it shows —
                                        Discord never shows it again. Direct messages use a non-privileged intent, so
                                        nothing under Privileged Gateway Intents needs turning on.
                                    </li>
                                </ol>
                                <Text variant="caption">
                                    After the channel is created you get an invite link. A person can only DM a bot they
                                    share a server with, so the bot has to be invited to a server your users are in.
                                </Text>
                            </CollapsibleContent>
                        </Collapsible>
                        {!agent && (
                            <FormSelect
                                control={form.control}
                                name="agentId"
                                label="Agent"
                                placeholder="Select an agent"
                                options={agentOptions}
                                description="Direct messages to this bot are answered by this agent."
                            />
                        )}
                        <FormInput
                            control={form.control}
                            name="botToken"
                            type="password"
                            label="Bot token"
                            placeholder="From the application's Bot page"
                            description="Validated with Discord and never shown again."
                        />
                        <FormInput
                            control={form.control}
                            name="displayName"
                            label="Channel name"
                            placeholder="Defaults to the bot's Discord username"
                            description="Shown in the channels list. Optional."
                        />
                        <ParameterBindingFields
                            control={form.control}
                            fields={parameterFields.fields}
                            rows={parameters}
                            sources={DISCORD_PARAMETER_SOURCES}
                            sourceHint={discordParameterSourceHint}
                            description="Map each agent parameter to a constant value bound once for the whole channel, or to the Discord user id or username of the sender of each message."
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
