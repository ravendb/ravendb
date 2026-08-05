import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { ApiState } from "@/components/data/api-state";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import type { FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";

// The backend binds this agent parameter to the Telegram sender automatically, so it is
// never asked for in the form (matched case-insensitively).
const USER_IDENTIFIER_PARAMETER = "useridentifier";

const telegramChannelSchema = z.object({
    agentId: z.string().min(1, "Select an agent to route conversations to"),
    displayName: z.string().trim(),
    botToken: z.string().trim().min(1, "Paste the bot token from @BotFather"),
    parameters: z.array(z.object({ name: z.string(), value: z.string().trim().min(1, "Required") })),
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
    const queryClient = useQueryClient();
    // The agent list is needed even with a fixed agent: its declared parameters drive the value inputs.
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    const form = useForm<TelegramChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(telegramChannelSchema),
        defaultValues: { agentId: agent?.agentId ?? "", displayName: "", botToken: "", parameters: [] },
    });

    const selectedAgentId = useWatch({ control: form.control, name: "agentId" });
    const parameterFields = useFieldArray({ control: form.control, name: "parameters" });

    const agents = agentsQuery.data ?? [];
    const selectedAgent = agents.find((candidate) => candidate.agentId === selectedAgentId);
    const hasUserIdentifier = (selectedAgent?.parameters ?? []).some(
        (name) => name.toLowerCase() === USER_IDENTIFIER_PARAMETER,
    );

    // Re-seed the value inputs whenever the agent selection changes its declared parameter set.
    const { replace } = parameterFields;
    useEffect(() => {
        const selected = (agentsQuery.data ?? []).find((candidate) => candidate.agentId === selectedAgentId);
        const names = (selected?.parameters ?? []).filter(
            (name) => name.toLowerCase() !== USER_IDENTIFIER_PARAMETER,
        );
        replace(names.map((name) => ({ name, value: "" })));
    }, [replace, selectedAgentId, agentsQuery.data]);

    const createMutation = useMutation({
        mutationFn: (values: TelegramChannelFormData) =>
            api.services.channels.create(slug, {
                type: "Telegram",
                agentId: values.agentId,
                allowedOrigins: null,
                displayName: values.displayName.trim() || null,
                botToken: values.botToken.trim(),
                parameters:
                    values.parameters.length > 0
                        ? Object.fromEntries(values.parameters.map(({ name, value }) => [name, value.trim()]))
                        : null,
            }),
        onSuccess: async () => {
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
                <ApiState
                    isLoading={agentsQuery.isPending}
                    isError={agentsQuery.isError}
                    errorTitle="Could not load agents"
                    onRetry={() => void agentsQuery.refetch()}
                    loadingLabel="Loading agents..."
                >
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
                                    {parameterFields.fields.map((field, index) => (
                                        <FormInput
                                            key={field.id}
                                            control={form.control}
                                            name={`parameters.${index}.value`}
                                            label={field.name}
                                            placeholder="e.g. customers/1"
                                            description={
                                                index === 0
                                                    ? "Agent parameters are bound once for the whole channel."
                                                    : undefined
                                            }
                                        />
                                    ))}
                                </div>
                            )}
                            {hasUserIdentifier && (
                                <p className="text-xs text-muted-foreground">
                                    The agent&apos;s <span className="font-medium">UserIdentifier</span> parameter is
                                    bound automatically to the Telegram user sending each message.
                                </p>
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
                </ApiState>
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
