import type { ReactNode } from "react";
import { useFormContext } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AgentParametersSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-parameters-section";
import { AgentQueryToolsSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-query-tools-section";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import AceEditor from "@/components/ace-editor/ace-editor";

const SYSTEM_PROMPT_PLACEHOLDER =
    "Describe the agent's purpose and capabilities. " +
    "E.g.: You are a customer support assistant for an e-commerce platform, " +
    "capable of answering questions about products and orders.";

export function AgentConfigurationTab() {
    const { control } = useFormContext<AgentFormData>();

    return (
        <div className="grid gap-8">
            <ConfigurationSection
                title="Configure basic settings"
                description="Define your agent's purpose, its AI provider connection, and the structure of its responses."
            >
                <div className="grid gap-5 rounded-lg border bg-background p-4">
                    <FormInput
                        control={control}
                        name="review.name"
                        label="Agent name"
                        placeholder="e.g. Customer Service Agent"
                    />
                    <FormInput
                        control={control}
                        name="review.identifier"
                        label="Identifier (optional)"
                        placeholder="e.g. customer-service-agent"
                        description="A unique identifier for the agent. Generated from the agent name when left empty."
                    />
                    <FormTextarea
                        control={control}
                        name="review.systemPrompt"
                        label="System prompt"
                        placeholder={SYSTEM_PROMPT_PLACEHOLDER}
                        rows={7}
                        description="Defines the agent's role and capabilities, guiding the LLM's responses throughout the conversation."
                    />
                    <FormAceEditor
                        control={control}
                        name="review.sampleObject"
                        mode="json"
                        label="Sample response object (optional)"
                        placeholder={`{\n    // "ResponseField": "Instruction to the LLM"\n}`}
                        description="A JSON object defining the structure of the responses you expect from the LLM. RavenDB generates the response JSON schema from it."
                        height="100px"
                        actions={[
                            { component: <AceEditor.FullScreenAction /> },
                            { component: <AceEditor.FormatAction /> },
                            { component: <AceEditor.AutoResizeHeightAction /> },
                        ]}
                    />
                    <FormAceEditor
                        control={control}
                        name="review.outputSchema"
                        mode="json"
                        label="Response JSON schema (optional)"
                        placeholder={`{\n    "type": "object",\n    "properties": { ... }\n}`}
                        description="Takes precedence over the sample response object when both are provided."
                        height="100px"
                        actions={[
                            { component: <AceEditor.FullScreenAction /> },
                            { component: <AceEditor.FormatAction /> },
                            { component: <AceEditor.AutoResizeHeightAction /> },
                        ]}
                    />
                </div>
            </ConfigurationSection>

            <ConfigurationSection
                title="Set agent parameters"
                description="Define query parameters that the agent will replace with fixed values before executing a query tool against the database."
            >
                <AgentParametersSection />
            </ConfigurationSection>

            <ConfigurationSection
                title="Define agent tools"
                description="Tools are a controlled way to pass context to the LLM."
            >
                <AgentQueryToolsSection />
            </ConfigurationSection>
        </div>
    );
}

function ConfigurationSection({
    title,
    description,
    children,
}: {
    title: string;
    description: string;
    children: ReactNode;
}) {
    return (
        <section className="grid gap-3">
            <div>
                <h3 className="text-sm font-semibold">{title}</h3>
                <p className="mt-1 text-xs text-muted-foreground">{description}</p>
            </div>
            {children}
        </section>
    );
}
