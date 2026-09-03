import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { ApiState } from "@/components/data/api-state";
import { FormFieldsSkeleton } from "@/components/data/loading-skeletons";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormStringList } from "@/components/form/form-string-list";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";

const webWidgetChannelSchema = z.object({
    agentId: z.string().min(1, "Select an agent to route conversations to"),
    displayName: z.string().trim(),
    allowedOrigins: z.array(z.object({ value: z.string().trim() })),
});

type WebWidgetChannelFormData = z.infer<typeof webWidgetChannelSchema>;

const DEFAULT_VALUES: WebWidgetChannelFormData = {
    agentId: "",
    displayName: "",
    allowedOrigins: [],
};

// The agent the channel routes to, when the caller has already chosen it (e.g. the capability
// wizard just created it). When omitted, the operator picks from the app's agents.
export type FixedAgent = { agentId: string; name: string };

export function WebWidgetChannelForm({
    slug,
    agent,
    onCreated,
}: {
    slug: string;
    agent?: FixedAgent;
    onCreated: () => void;
}) {
    const queryClient = useQueryClient();
    // With a fixed agent there's nothing to pick, so skip loading the list.
    const agentsQuery = useQuery({ ...api.queries.agents.list(slug), enabled: !agent });

    const form = useForm<WebWidgetChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(webWidgetChannelSchema),
        defaultValues: { ...DEFAULT_VALUES, agentId: agent?.agentId ?? "" },
    });

    const unsavedChanges = useFormUnsavedChanges(form);

    const createMutation = useMutation({
        mutationFn: (values: WebWidgetChannelFormData) =>
            api.services.channels.create(slug, {
                type: "IFrame",
                agentId: values.agentId,
                displayName: values.displayName.trim() || null,
                allowedOrigins: values.allowedOrigins.map((origin) => origin.value.trim()).filter(Boolean),
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug, "IFrame");
            toast.success("Web widget channel created");
            onCreated();
        },
    });

    const agents = agentsQuery.data ?? [];
    const agentOptions: FormSelectOption<string>[] = agents.map((option) => ({
        value: option.agentId,
        label: option.name,
    }));
    // A fixed agent always gives a target; otherwise the operator needs at least one to pick.
    const hasAgentTarget = Boolean(agent?.agentId) || agents.length > 0;

    return (
        <form
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={withNestedSubmit(form.handleSubmit((values) => createMutation.mutate(values)))}
        >
            <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                <ApiState
                    isLoading={!agent && agentsQuery.isPending}
                    isError={!agent && agentsQuery.isError}
                    errorTitle="Could not load agents"
                    onRetry={() => void agentsQuery.refetch()}
                    loadingLabel="Loading agents..."
                    skeleton={<FormFieldsSkeleton count={3} />}
                >
                    {!hasAgentTarget ? (
                        <Alert>
                            Create an agent first — a channel routes conversations to one of the app&apos;s agents.
                        </Alert>
                    ) : (
                        <>
                            {/* When the agent is fixed it's bound via the form's defaults; only show the
                                picker when the operator still needs to choose one. */}
                            {!agent && (
                                <FormSelect
                                    control={form.control}
                                    name="agentId"
                                    label="Agent"
                                    placeholder="Select an agent"
                                    options={agentOptions}
                                    description="Conversations from this widget are answered by this agent."
                                />
                            )}
                            <FormInput
                                control={form.control}
                                name="displayName"
                                label="Channel name"
                                placeholder="e.g. Storefront help"
                                description="Shown in the channels list. Optional."
                            />
                            <FormStringList
                                control={form.control}
                                name="allowedOrigins"
                                label="Allowed origins"
                                description="The widget only loads on these origins. Leave empty to allow any site."
                                addButtonLabel="Add origin"
                                emptyLabel="No origins — the widget can be embedded on any site."
                                defaultValue={{ value: "" }}
                                fieldName={(index) => `allowedOrigins.${index}.value`}
                                itemLabel={(index) => `Origin ${index + 1}`}
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
