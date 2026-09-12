import { CircleQuestionMark } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";

export function InfoHint({ content }: { content: string }) {
    return (
        <TooltipProvider>
            <Tooltip>
                <TooltipTrigger asChild>
                    {/* Faded rather than muted, so the hint reads as secondary to whatever colour
                        the label beside it already has. */}
                    <span className="inline-flex opacity-[0.66]">
                        <CircleQuestionMark className="size-3.5" aria-hidden="true" />
                        <span className="sr-only">{content}</span>
                    </span>
                </TooltipTrigger>
                <TooltipContent>{content}</TooltipContent>
            </Tooltip>
        </TooltipProvider>
    );
}
