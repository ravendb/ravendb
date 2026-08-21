import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { cn } from "@/lib/utils";
import type { LogDestination } from "./log-settings-summary";

const TONE_DOT: Record<LogDestination["tone"], string> = {
    positive: "bg-badge-success-fg",
    warning: "bg-badge-warning-fg",
    muted: "bg-muted-foreground/50",
    info: "bg-badge-info-fg",
    danger: "bg-destructive",
    loading: "bg-muted-foreground/50",
};

/**
 * Where output goes right now, above the fold, so the page opens with an answer rather than a form.
 *
 * Each chip carries a hint, because a label and a value cannot say what the destination is or what
 * the value means - "Audit · No trail" does not explain what stops being recorded. The hint is also
 * rendered for assistive tech, matching `InfoHint`, so it does not depend on hovering.
 */
export function LogDestinations({ destinations }: { destinations: LogDestination[] }) {
    return (
        <TooltipProvider>
            <dl className="flex flex-wrap gap-2">
                {destinations.map((destination) => (
                    <Tooltip key={destination.key}>
                        <TooltipTrigger asChild>
                            <div className="flex min-w-0 items-center gap-2 rounded-lg border bg-card px-3 py-1.5">
                                <span
                                    className={cn("size-2 shrink-0 rounded-full", TONE_DOT[destination.tone])}
                                    aria-hidden="true"
                                />
                                <dt className="text-xs text-muted-foreground">{destination.label}</dt>
                                <dd className="truncate text-xs font-medium">{destination.value}</dd>
                                <span className="sr-only">{destination.hint}</span>
                            </div>
                        </TooltipTrigger>
                        <TooltipContent className="max-w-xs">{destination.hint}</TooltipContent>
                    </Tooltip>
                ))}
            </dl>
        </TooltipProvider>
    );
}
