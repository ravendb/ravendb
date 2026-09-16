import { AiProgressStatus, type AiProgressStage } from "@/components/data/ai-progress-status";
import { SuggestedAgentCardsSkeleton } from "@/pages/setup/add-capability-wizard/steps/create/suggested-agents-skeleton";

const SUGGESTION_STAGES: AiProgressStage[] = [
    { fromSeconds: 0, label: "Thinking" },
    { fromSeconds: 5, label: "Reading your collections" },
    { fromSeconds: 15, label: "Looking at the shape of your data" },
    { fromSeconds: 30, label: "Drafting agent ideas" },
    { fromSeconds: 55, label: "Writing system prompts" },
    { fromSeconds: 85, label: "Almost done" },
    { fromSeconds: 120, label: "Still working on it" },
];

export function SuggestedAgentsProgress({ startedAt }: { startedAt: number | undefined }) {
    return (
        <div className="grid gap-4 rounded-lg border bg-background p-4">
            <AiProgressStatus stages={SUGGESTION_STAGES} startedAt={startedAt}>
                We&apos;re analyzing your collections to propose agents. This usually takes a minute or two - you can
                describe your own agent below in the meantime.
            </AiProgressStatus>

            <SuggestedAgentCardsSkeleton />
        </div>
    );
}
