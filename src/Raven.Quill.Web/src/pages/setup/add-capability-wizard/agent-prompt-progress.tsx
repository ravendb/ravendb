import { AiProgressStatus, type AiProgressStage } from "@/components/data/ai-progress-status";

const PROMPT_STAGES: AiProgressStage[] = [
    { fromSeconds: 0, label: "Reading your description" },
    { fromSeconds: 5, label: "Looking at your collections" },
    { fromSeconds: 20, label: "Writing the system prompt" },
    { fromSeconds: 45, label: "Designing query tools" },
    { fromSeconds: 80, label: "Almost done" },
    { fromSeconds: 120, label: "Still working on it" },
];

/** Shown while `generateAgentFromPrompt` runs, both on the create step and on the review step. */
export function AgentPromptProgress() {
    return (
        <div className="rounded-lg border border-dashed bg-muted/30 p-4">
            <AiProgressStatus stages={PROMPT_STAGES}>
                We&apos;re turning your description into an agent configuration. This usually takes a minute or two.
            </AiProgressStatus>
        </div>
    );
}
