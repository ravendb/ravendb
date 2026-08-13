import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm, type FieldPath } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { ChevronDown } from "lucide-react";
import { Link, useNavigate } from "react-router";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AiAgentConfiguration, AiConnectionString, WebhookBinding } from "@/api/generated/server-api";
import { AddAiConnectionString } from "@/components/ai-connection-string/add-ai-connection-string";
import {
    getConnectionStringLabel,
    getServerConnectionStringName,
} from "@/components/ai-connection-string/ai-connection-string-utils";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { FormCombobox } from "@/components/form/form-combobox";
import { FormErrorIcon } from "@/components/form/form-error-icon";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { appRoutes } from "@/lib/app-routes";
import { invalidateAgentQueries } from "@/lib/query-invalidation";
import {
    buildActionBindings,
    buildAgentConfigurationPayload,
    suggestionToAgentConfiguration,
} from "@/pages/setup/add-capability-wizard/agent-config-form";
import { agentSchema } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AgentActionsSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-actions-section";
import { SYSTEM_PROMPT_PLACEHOLDER } from "@/pages/setup/add-capability-wizard/steps/review/agent-configuration-tab";
import { AgentParametersSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-parameters-section";
import { AgentQueryToolsSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-query-tools-section";
import { ReviewTestAgentButton } from "@/pages/setup/add-capability-wizard/steps/review/test-agent-sheet";

const editAgentSchema = agentSchema.pick({ connection: true, review: true });
type EditAgentFormData = z.infer<typeof editAgentSchema>;
type SectionId = "basic" | "parameters" | "tools" | "actions";

const SECTION_FIELDS: Record<SectionId, readonly FieldPath<EditAgentFormData>[]> = {
    basic: ["review.name", "connection.connectionStringName", "review.systemPrompt"],
    parameters: ["review.parameters"],
    tools: ["review.queries"],
    actions: ["review.actions"],
};

const ALL_SECTIONS_OPEN: Record<SectionId, boolean> = {
    basic: true,
    parameters: true,
    tools: true,
    actions: true,
};

type EditAgentFormProps = {
    slug: string;
    agentId: string;
    config: AiAgentConfiguration;
    actionBindings: Record<string, WebhookBinding>;
    connectionStrings: AiConnectionString[];
};

export function EditAgentForm({ slug, agentId, config, actionBindings, connectionStrings }: EditAgentFormProps) {
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const [openSections, setOpenSections] = useState<Record<SectionId, boolean>>(ALL_SECTIONS_OPEN);

    const form = useForm<EditAgentFormData>({
        mode: "onChange",
        resolver: zodResolver(editAgentSchema),
        defaultValues: {
            connection: { connectionStringName: config.connectionStringName ?? "" },
            review: suggestionToAgentConfiguration(config, actionBindings),
        },
    });

    const unsavedChanges = useFormUnsavedChanges(form);

    const updateMutation = useMutation({
        mutationFn: (values: EditAgentFormData) =>
            // The edit endpoint replaces the whole configuration, so start from the fetched
            // one and re-apply the parts this form doesn't edit (identifier, sub-agents,
            // disabled) that the payload builder would otherwise reset.
            api.services.agents.edit(slug, {
                configuration: {
                    ...config,
                    ...buildAgentConfigurationPayload(values),
                    identifier: config.identifier,
                    subAgents: config.subAgents ?? [],
                    disabled: config.disabled ?? false,
                },
                actionBindings: buildActionBindings(values),
            }),
        onSuccess: async (_result, values) => {
            unsavedChanges.markSaved();
            await Promise.all([
                invalidateAgentQueries(queryClient, slug),
                queryClient.invalidateQueries({
                    queryKey: api.queries.agents.detail(slug, agentId).queryKey,
                }),
            ]);
            toast.success(`Agent "${values.review.name.trim()}" updated`);
            void navigate(appRoutes.app(slug, "agents"));
        },
    });

    // Validation errors may live in a collapsed section; reveal them all on a blocked save.
    const openAllSections = () => setOpenSections(ALL_SECTIONS_OPEN);

    const setSectionOpen = (section: SectionId) => (isOpen: boolean) =>
        setOpenSections((sections) => ({ ...sections, [section]: isOpen }));

    return (
        <FormProvider {...form}>
            <form
                className="flex min-h-0 flex-1 flex-col gap-3"
                onSubmit={form.handleSubmit((values) => updateMutation.mutate(values), openAllSections)}
                noValidate
            >
                <div className="flex min-h-0 flex-1 flex-col gap-8 overflow-y-auto">
                    <CollapsibleSection
                        title="Basic settings"
                        description="The agent's purpose and its AI provider connection."
                        errorIcon={<FormErrorIcon control={form.control} paths={SECTION_FIELDS.basic} />}
                        isOpen={openSections.basic}
                        onOpenChange={setSectionOpen("basic")}
                    >
                        <div className="grid gap-5 rounded-lg border bg-card p-4">
                            <FormInput
                                control={form.control}
                                name="review.name"
                                label="Agent name"
                                placeholder="e.g. Customer Service Agent"
                            />
                            <FormCombobox
                                control={form.control}
                                name="connection.connectionStringName"
                                label="Connection string"
                                placeholder="Select..."
                                options={connectionStrings.map((item) => ({
                                    value: item.name ?? "",
                                    label: getConnectionStringLabel(item),
                                }))}
                                addons={
                                    <AddAiConnectionString
                                        modelType="Chat"
                                        onCreated={(name) =>
                                            form.setValue(
                                                "connection.connectionStringName",
                                                getServerConnectionStringName(name),
                                                {
                                                    shouldValidate: true,
                                                    shouldDirty: true,
                                                },
                                            )
                                        }
                                    />
                                }
                            />
                            <FormTextarea
                                control={form.control}
                                name="review.systemPrompt"
                                label="System prompt"
                                placeholder={SYSTEM_PROMPT_PLACEHOLDER}
                                rows={7}
                                description="Defines the agent's role and capabilities, guiding the LLM's responses throughout the conversation."
                            />
                        </div>
                    </CollapsibleSection>

                    <CollapsibleSection
                        title="Agent parameters"
                        description="Query parameters that the agent will replace with fixed values before executing a query tool against the database."
                        errorIcon={<FormErrorIcon control={form.control} paths={SECTION_FIELDS.parameters} />}
                        isOpen={openSections.parameters}
                        onOpenChange={setSectionOpen("parameters")}
                    >
                        <AgentParametersSection className="bg-card" />
                    </CollapsibleSection>

                    <CollapsibleSection
                        title="Agent tools"
                        description="Tools are a controlled way to pass context to the LLM."
                        errorIcon={<FormErrorIcon control={form.control} paths={SECTION_FIELDS.tools} />}
                        isOpen={openSections.tools}
                        onOpenChange={setSectionOpen("tools")}
                    >
                        <AgentQueryToolsSection className="bg-card" />
                    </CollapsibleSection>

                    <CollapsibleSection
                        title="Agent actions"
                        description="Actions let the agent call an external webhook during a conversation and use its response."
                        errorIcon={<FormErrorIcon control={form.control} paths={SECTION_FIELDS.actions} />}
                        isOpen={openSections.actions}
                        onOpenChange={setSectionOpen("actions")}
                    >
                        <AgentActionsSection className="bg-card" />
                    </CollapsibleSection>
                </div>

                {updateMutation.isError && (
                    <Alert variant="destructive">
                        {updateMutation.error instanceof Error
                            ? updateMutation.error.message
                            : "Could not update agent."}
                    </Alert>
                )}

                <div className="flex items-center justify-between gap-2 border-t pt-3">
                    <Button asChild type="button" variant="outline">
                        <Link to={appRoutes.app(slug, "agents")}>Cancel</Link>
                    </Button>
                    <div className="flex gap-2">
                        <ReviewTestAgentButton isBusy={updateMutation.isPending} />
                        <Button type="submit" disabled={updateMutation.isPending}>
                            {updateMutation.isPending && <Spinner />}
                            Save changes
                        </Button>
                    </div>
                </div>
            </form>
        </FormProvider>
    );
}

function CollapsibleSection({
    title,
    description,
    errorIcon,
    isOpen,
    onOpenChange,
    children,
}: {
    title: string;
    description: string;
    errorIcon?: ReactNode;
    isOpen: boolean;
    onOpenChange: (isOpen: boolean) => void;
    children: ReactNode;
}) {
    return (
        <Collapsible open={isOpen} onOpenChange={onOpenChange} className="grid gap-3">
            <CollapsibleTrigger className="group flex w-full items-start justify-between gap-3 text-left">
                <div>
                    <h3 className="flex items-center gap-1.5 text-sm font-semibold">
                        {title}
                        {errorIcon}
                    </h3>
                    <p className="mt-1 text-xs text-muted-foreground">{description}</p>
                </div>
                <ChevronDown
                    className="mt-0.5 size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                    aria-hidden="true"
                />
            </CollapsibleTrigger>
            <CollapsibleContent className="grid gap-3">{children}</CollapsibleContent>
        </Collapsible>
    );
}
