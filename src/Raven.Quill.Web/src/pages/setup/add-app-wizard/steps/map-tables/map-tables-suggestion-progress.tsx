import { AiProgressStatus, type AiProgressStage } from "@/components/data/ai-progress-status";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { cn } from "@/lib/utils";

const SUGGESTION_STAGES: AiProgressStage[] = [
    { fromSeconds: 0, label: "Thinking" },
    { fromSeconds: 5, label: "Reading your schema" },
    { fromSeconds: 15, label: "Tracing relationships" },
    { fromSeconds: 30, label: "Grouping tables" },
    { fromSeconds: 55, label: "Drafting the mapping" },
    { fromSeconds: 85, label: "Almost done" },
    { fromSeconds: 120, label: "Still working on it" },
];

const EXPLORER_ROW_WIDTHS = ["w-full", "w-11/12", "w-10/12", "w-full", "w-9/12", "w-11/12", "w-8/12"];

export function MapTablesSuggestionProgress({ startedAt }: { startedAt: number | undefined }) {
    return (
        <div className="flex min-h-80 flex-1 flex-col gap-6 rounded-lg border bg-background p-6">
            <AiProgressStatus stages={SUGGESTION_STAGES} startedAt={startedAt}>
                We&apos;re proposing a mapping for the tables you verified. This usually takes a minute or two.
            </AiProgressStatus>

            <div className="grid flex-1 gap-6 sm:grid-cols-[minmax(0,18rem)_minmax(0,1fr)]">
                <div className="grid content-start gap-2">
                    {EXPLORER_ROW_WIDTHS.map((width, index) => (
                        <Skeleton key={index} className={cn("h-8", width)} />
                    ))}
                </div>
                <div className="grid content-start gap-3">
                    <Skeleton className="h-8 w-1/2" />
                    <Skeleton className="h-24 w-full" />
                    <Skeleton className="h-8 w-2/3" />
                    <Skeleton className="h-24 w-full" />
                </div>
            </div>
        </div>
    );
}
