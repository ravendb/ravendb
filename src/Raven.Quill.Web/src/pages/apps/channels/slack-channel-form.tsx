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
import { Heading, Text } from "@/components/typography";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { Separator } from "@/components/shadcn/ui/separator";
import { ApiState } from "@/components/data/api-state";
import { CodeBlockTabs } from "@/components/data/code-block-tabs";
import { NumberedSteps } from "@/components/data/numbered-steps";
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
import { SLACK_APP_MANIFEST } from "@/pages/apps/channels/slack-app-manifest";
import {
    SLACK_PARAMETER_SOURCES,
    SLACK_SOURCE_VALUES,
    slackParameterSourceHint,
} from "@/pages/apps/channels/slack-parameter-sources";
import { SlackConnectionCard } from "@/pages/apps/channels/slack-connection-card";
import { SlackWebhookPanel } from "@/pages/apps/channels/slack-webhook-panel";
import type { FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";

const parameterBindingSchema = z
    .object({
        name: z.string(),
        source: z.enum(SLACK_SOURCE_VALUES),
        value: z.string().trim(),
    })
    .superRefine((parameter, ctx) => {
        if (parameter.source === "Constant" && parameter.value.trim().length === 0) {
            ctx.addIssue({ code: "custom", message: "Required", path: ["value"] });
        }
    });

const slackChannelSchema = z.object({
    agentId: z.string().min(1, "Select an agent to route conversations to"),
    displayName: z.string().trim(),
    botToken: z
        .string()
        .trim()
        .min(1, "Paste the bot token from the Slack app's OAuth page")
        .startsWith("xoxb-", "The bot token starts with xoxb- (not a user or app-level token)"),
    signingSecret: z.string().trim().min(1, "Paste the signing secret (Basic Information > App Credentials)"),
    parameters: z.array(parameterBindingSchema),
});

type SlackChannelFormData = z.infer<typeof slackChannelSchema>;

export function SlackChannelForm({ slug, agent, onDone }: { slug: string; agent?: FixedAgent; onDone: () => void }) {
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

    return <LoadedSlackChannelForm slug={slug} agent={agent} agents={agentsQuery.data} onDone={onDone} />;
}

function LoadedSlackChannelForm({
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
    const [isManifestOpen, setIsManifestOpen] = useState(false);

    const form = useForm<SlackChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(slackChannelSchema),
        defaultValues: {
            agentId: agent?.agentId ?? "",
            displayName: "",
            botToken: "",
            signingSecret: "",
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
        mutationFn: (values: SlackChannelFormData) =>
            api.services.channels.create(slug, {
                type: "Slack",
                agentId: values.agentId,
                allowedOrigins: null,
                displayName: values.displayName.trim() || null,
                slack: {
                    botToken: values.botToken.trim(),
                    signingSecret: values.signingSecret.trim(),
                    parameterBindings: values.parameters.length > 0 ? toParameterBindings(values.parameters) : null,
                },
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug, "Slack");
            toast.success("Slack channel created");
        },
    });

    const createdChannelId = createMutation.data?.channelId;
    if (createdChannelId) {
        return (
            <div className="flex min-h-0 flex-1 flex-col">
                <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                    <div className="space-y-1">
                        <Heading as="h3" variant="subsection">
                            Bot connected
                        </Heading>
                        <Text variant="muted">Now finish the event subscription so Slack delivers messages to it.</Text>
                    </div>
                    <SlackConnectionCard slug={slug} channelId={createdChannelId} />
                    <SlackWebhookPanel slug={slug} channelId={createdChannelId} />
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
                        <Collapsible open={isManifestOpen} onOpenChange={setIsManifestOpen} className="grid gap-3">
                            <CollapsibleTrigger className="group flex w-full items-start justify-between gap-3 text-left">
                                <div>
                                    <Heading as="h3" variant="label">
                                        No Slack app yet?
                                    </Heading>
                                    <Text variant="caption" className="mt-1">
                                        Create one from this manifest, install it to your workspace, then paste its
                                        credentials below.
                                    </Text>
                                </div>
                                <ChevronDown
                                    className="mt-0.5 size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                                    aria-hidden="true"
                                />
                            </CollapsibleTrigger>
                            <CollapsibleContent>
                                <NumberedSteps
                                    size="sm"
                                    steps={[
                                        {
                                            title: "Create the app from this manifest",
                                            content: (
                                                <div className="space-y-2">
                                                    <Text variant="caption">
                                                        At{" "}
                                                        <a
                                                            href="https://api.slack.com/apps"
                                                            target="_blank"
                                                            rel="noreferrer"
                                                            className="underline"
                                                        >
                                                            api.slack.com/apps
                                                        </a>
                                                        , choose{" "}
                                                        <span className="font-medium">
                                                            Create New App &gt; From a manifest
                                                        </span>{" "}
                                                        and paste:
                                                    </Text>
                                                    <CodeBlockTabs
                                                        tabs={[
                                                            {
                                                                value: "manifest",
                                                                label: "Manifest (YAML)",
                                                                code: SLACK_APP_MANIFEST,
                                                                language: "yaml",
                                                            },
                                                        ]}
                                                        value="manifest"
                                                        copyLabel="Copy app manifest"
                                                    />
                                                </div>
                                            ),
                                        },
                                        {
                                            title: "Install it and copy the credentials",
                                            content: (
                                                <Text variant="caption">
                                                    <span className="font-medium">Install to Workspace</span>. The bot
                                                    token is on the OAuth &amp; Permissions page, the signing secret
                                                    under Basic Information.
                                                </Text>
                                            ),
                                        },
                                    ]}
                                />
                            </CollapsibleContent>
                        </Collapsible>

                        <Separator />

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
                            placeholder="xoxb-..."
                            description="From the app's OAuth & Permissions page. Validated with Slack and never shown again."
                        />
                        <FormInput
                            control={form.control}
                            name="signingSecret"
                            type="password"
                            label="Signing secret"
                            placeholder="From Basic Information > App Credentials"
                            description="Verifies that event deliveries really come from Slack. Never shown again."
                        />
                        <FormInput
                            control={form.control}
                            name="displayName"
                            label="Channel name"
                            placeholder="Defaults to the bot's Slack name"
                            description="Shown in the channels list. Optional."
                        />
                        <ParameterBindingFields
                            control={form.control}
                            fields={parameterFields.fields}
                            rows={parameters}
                            sources={SLACK_PARAMETER_SOURCES}
                            sourceHint={slackParameterSourceHint}
                            description="Map each agent parameter to a constant value bound once for the whole channel, or to the Slack user id or email of the sender of each message."
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
