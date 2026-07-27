import { AiProgressStatus, type AiProgressStage } from "@/components/data/ai-progress-status";
import { Skeleton } from "@/components/shadcn/ui/skeleton";

const SUGGESTION_STAGES: AiProgressStage[] = [
    { fromSeconds: 0, label: "Thinking" },
    { fromSeconds: 5, label: "Reading your collections" },
    { fromSeconds: 15, label: "Looking at the shape of your data" },
    { fromSeconds: 30, label: "Drafting agent ideas" },
    { fromSeconds: 55, label: "Writing system prompts" },
    { fromSeconds: 85, label: "Almost done" },
    { fromSeconds: 120, label: "Still working on it" },
];

export function SuggestedAgentsProgress() {
    return (
        <div className="grid gap-4 rounded-lg border bg-background p-4">
            <AiProgressStatus stages={SUGGESTION_STAGES}>
                We&apos;re analyzing your collections to propose agents. This usually takes a minute or two - you can
                describe your own agent below in the meantime.
            </AiProgressStatus>

            <div className="grid auto-cols-[minmax(0,1fr)] grid-flow-col gap-3">
                {Array.from({ length: 3 }).map((_, index) => (
                    <div key={index} className="grid min-h-28 content-start gap-2 rounded-lg border p-4">
                        <Skeleton className="h-4 w-2/3" />
                        <Skeleton className="h-3 w-full" />
                        <Skeleton className="h-3 w-11/12" />
                        <Skeleton className="h-3 w-3/4" />
                    </div>
                ))}
            </div>
        </div>
    );
}
