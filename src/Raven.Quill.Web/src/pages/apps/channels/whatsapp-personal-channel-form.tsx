import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { TelegramParameterBinding, TelegramParameterSource } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { ApiState } from "@/components/data/api-state";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import { WhatsAppPairingPanel } from "@/pages/apps/channels/whatsapp-pairing-panel";
import type { FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";

// A WhatsApp sender is identified only by its phone number, so Telegram's user id and
// username sources have nothing to bind to here.
const WHATSAPP_PARAMETER_SOURCES: FormSelectOption<TelegramParameterSource>[] = [
    { value: "Constant", label: "Constant value" },
    { value: "PhoneNumber", label: "Sender phone number" },
];

const parameterBindingSchema = z
    .object({
        name: z.string(),
        source: z.enum(["Constant", "PhoneNumber"]),
        value: z.string().trim(),
    })
    .superRefine((parameter, ctx) => {
        if (parameter.source === "Constant" && parameter.value.trim().length === 0) {
            ctx.addIssue({ code: "custom", message: "Required", path: ["value"] });
        }
    });

const whatsAppChannelSchema = z.object({
    agentId: z.string().min(1, "Select an agent to route conversations to"),
    displayName: z.string().trim(),
    parameters: z.array(parameterBindingSchema),
});

type WhatsAppChannelFormData = z.infer<typeof whatsAppChannelSchema>;

function toParameterBindings(parameters: WhatsAppChannelFormData["parameters"]) {
    const bindings: Record<string, TelegramParameterBinding> = {};
    for (const { name, source, value } of parameters) {
        bindings[name] = { source, value: source === "Constant" ? value.trim() : null };
    }
    return bindings;
}

// Two phases in one sheet: the create form, then the pairing panel for the freshly
// provisioned channel — the operator has their phone in hand, so no detour via the
// channel page. Pairing can always be finished later from there.
export function WhatsAppPersonalChannelForm({
    slug,
    agent,
    onDone,
}: {
    slug: string;
    agent?: FixedAgent;
    onDone: () => void;
}) {
    const queryClient = useQueryClient();
    const agentsQuery = useQuery({ ...api.queries.agents.list(slug), enabled: !agent });

    const form = useForm<WhatsAppChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(whatsAppChannelSchema),
        defaultValues: { agentId: agent?.agentId ?? "", displayName: "", parameters: [] },
    });

    const selectedAgentId = useWatch({ control: form.control, name: "agentId" });
    const parameterFields = useFieldArray({ control: form.control, name: "parameters" });
    const parameters = useWatch({ control: form.control, name: "parameters" }) ?? [];

    const agents = agentsQuery.data ?? [];

    const { replace } = parameterFields;
    useEffect(() => {
        const selected = (agentsQuery.data ?? []).find((candidate) => candidate.agentId === selectedAgentId);
        const names = selected?.parameters ?? [];
        replace(names.map((name) => ({ name, source: "Constant" as const, value: "" })));
    }, [replace, selectedAgentId, agentsQuery.data]);

    const createMutation = useMutation({
        mutationFn: (values: WhatsAppChannelFormData) =>
            api.services.channels.create(slug, {
                type: "WhatsAppPersonal",
                agentId: values.agentId,
                allowedOrigins: null,
                displayName: values.displayName.trim() || null,
                whatsApp: {
                    parameterBindings: values.parameters.length > 0 ? toParameterBindings(values.parameters) : null,
                },
            }),
        onSuccess: async () => {
            await invalidateChannelQueries(queryClient, slug);
            toast.success("WhatsApp Personal channel created");
        },
    });

    const createdChannelId = createMutation.data?.channelId;
    if (createdChannelId) {
        return (
            <>
                <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                    <WhatsAppPairingPanel slug={slug} channelId={createdChannelId} />
                    <p className="text-xs text-muted-foreground">
                        You can finish pairing later from the channel&apos;s page.
                    </p>
                </div>
                <SheetFooter className="flex-row justify-end border-t">
                    <SheetClose asChild>
                        <Button type="button" onClick={onDone}>
                            Done
                        </Button>
                    </SheetClose>
                </SheetFooter>
            </>
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
                <ApiState
                    isLoading={!agent && agentsQuery.isPending}
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
                                    description="Messages to the linked phone are answered by this agent."
                                />
                            )}
                            <FormInput
                                control={form.control}
                                name="displayName"
                                label="Channel name"
                                placeholder="WhatsApp Personal"
                                description="Shown in the channels list. Optional."
                            />
                            {parameterFields.fields.length > 0 && (
                                <div className="flex flex-col gap-3">
                                    <p className="text-xs text-muted-foreground">
                                        Map each agent parameter to a constant value bound once for the whole channel,
                                        or to the phone number sending each message.
                                    </p>
                                    {parameterFields.fields.map((field, index) => (
                                        <div key={field.id} className="grid gap-2 sm:grid-cols-2">
                                            <FormSelect
                                                control={form.control}
                                                name={`parameters.${index}.source`}
                                                label={field.name}
                                                options={WHATSAPP_PARAMETER_SOURCES}
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
                                    ))}
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
                    Create channel
                </Button>
            </SheetFooter>
        </form>
    );
}
