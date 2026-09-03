import { type ReactNode } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Sparkles } from "lucide-react";
import { Text } from "@/components/typography";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { ExpandableText } from "@/components/data/expandable-text";
import { FormTextarea } from "@/components/form/form-textarea";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AgentPromptProgress } from "@/pages/setup/add-capability-wizard/agent-prompt-progress";
import { SuggestionPicker } from "@/pages/setup/add-capability-wizard/suggestion-picker";
import { useRegenerateAgentFromPromptMutation } from "@/pages/setup/add-capability-wizard/steps/review/use-regenerate-agent-from-prompt";

// Overview of the AI-generated agent: choose a data candidate ("ai" mode) or edit the prompt
// and regenerate ("prompt" mode), then preview the resulting configuration. All summary values
// come from the editable configuration, so edits made in the "Agent configuration" tab show here.
export function AgentSuggestionTab() {
    const { control } = useFormContext<AgentFormData>();
    const mode = useWatch({ control, name: "create.mode" });
    const config = useWatch({ control, name: "review" });
    const connectionStringName = useWatch({ control, name: "connection.connectionStringName" });

    const systemPrompt = config.systemPrompt?.trim() ?? "";
    const parameterNames = (config.parameters ?? []).map((parameter) => parameter.name).filter(Boolean);
    const queryToolNames = (config.queries ?? []).map((query) => query.name).filter(Boolean);

    return (
        <div className="grid gap-5">
            {mode === "prompt" ? <PromptEditor /> : <SuggestionPicker />}

            <div className="grid gap-2">
                <Text variant="label" as="div" className="flex items-center gap-2">
                    <Sparkles className="size-4" />
                    Agent summary
                </Text>
                <div className="flex flex-col divide-y rounded-lg border bg-background px-4">
                    <SummaryRow label="Agent name">{config.name || "—"}</SummaryRow>
                    <SummaryRow label="System prompt">
                        {systemPrompt ? (
                            <ExpandableText maxLines={3} className="text-justify text-sm whitespace-pre-wrap">
                                {systemPrompt}
                            </ExpandableText>
                        ) : (
                            <Text variant="muted" as="span">
                                —
                            </Text>
                        )}
                    </SummaryRow>
                    <SummaryRow label="Connection string">{connectionStringName || "—"}</SummaryRow>
                    <SummaryRow label="Parameters">
                        <ChipList items={parameterNames} />
                    </SummaryRow>
                    <SummaryRow label="Query tools">
                        <ChipList items={queryToolNames} />
                    </SummaryRow>
                </div>
            </div>
        </div>
    );
}

// Editable copy of the prompt that produced this agent. Regenerating replaces the
// configuration with a fresh candidate built from the edited text.
function PromptEditor() {
    const { control } = useFormContext<AgentFormData>();
    const promptInput = useWatch({ control, name: "create.promptInput" });
    const regenerate = useRegenerateAgentFromPromptMutation();

    const trimmedPrompt = (promptInput ?? "").trim();

    const regenerateFromPrompt = () => {
        if (!trimmedPrompt || regenerate.isPending) {
            return;
        }

        regenerate.mutate(trimmedPrompt);
    };

    return (
        <div className="grid gap-3 rounded-lg border bg-background p-4">
            <FormTextarea
                control={control}
                name="create.promptInput"
                label="Your prompt"
                description="Edit your description and regenerate to get an updated agent configuration."
                rows={4}
                disabled={regenerate.isPending}
            />
            {regenerate.isPending && <AgentPromptProgress />}
            <Button
                type="button"
                variant="secondary"
                className="justify-self-start"
                onClick={regenerateFromPrompt}
                disabled={!trimmedPrompt || regenerate.isPending}
            >
                {regenerate.isPending ? <Spinner /> : <Sparkles className="size-4" />}
                {regenerate.isPending ? "Regenerating..." : "Regenerate"}
            </Button>
        </div>
    );
}

function SummaryRow({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div className="flex min-h-12 items-center justify-between gap-12 py-3">
            <Text variant="label" as="span" className="shrink-0">
                {label}
            </Text>
            <div className="flex min-w-0 justify-end text-sm">{children}</div>
        </div>
    );
}

function ChipList({ items }: { items: string[] }) {
    if (items.length === 0) {
        return <span className="text-muted-foreground">—</span>;
    }

    return (
        <div className="flex flex-wrap justify-end gap-1.5">
            {items.map((item) => (
                <Badge key={item} variant="secondary" className="font-mono">
                    {item}
                </Badge>
            ))}
        </div>
    );
}
