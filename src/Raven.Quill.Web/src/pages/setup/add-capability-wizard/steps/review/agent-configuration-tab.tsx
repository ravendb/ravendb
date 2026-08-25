import type { ReactNode } from "react";
import { useFormContext } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AgentActionsSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-actions-section";
import { AgentParametersSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-parameters-section";
import { AgentQueryToolsSection } from "@/pages/setup/add-capability-wizard/steps/review/agent-query-tools-section";
import { SectionHeader } from "@/components/section-header";

export const SYSTEM_PROMPT_PLACEHOLDER =
    "Describe the agent's purpose and capabilities. " +
    "E.g.: You are a customer support assistant for an e-commerce platform, " +
    "capable of answering questions about products and orders.";

export function AgentConfigurationTab() {
    const { control } = useFormContext<AgentFormData>();

    return (
        <div className="grid gap-8">
            <ConfigurationSection
                title="Configure basic settings"
                description="Define your agent's purpose and its AI provider connection."
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

            <ConfigurationSection
                title="Define agent actions"
                description="Actions let the agent call an external webhook during a conversation and use its response."
            >
                <AgentActionsSection />
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
            <SectionHeader level="section" title={title} description={description} />
            {children}
        </section>
    );
}
