import { CircleQuestionMark } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";

export function InfoHint({ content }: { content: string }) {
    return (
        <TooltipProvider>
            <Tooltip>
                <TooltipTrigger asChild>
                    {/* Faded rather than muted, so the hint reads as secondary to whatever colour
                        the label beside it already has. */}
                    <button
                        type="button"
                        className="inline-flex cursor-help rounded-sm opacity-[0.66] outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset"
                        aria-label="More information"
                    >
                        <CircleQuestionMark className="size-3.5" aria-hidden="true" />
                    </button>
                </TooltipTrigger>
                <TooltipContent>{content}</TooltipContent>
            </Tooltip>
        </TooltipProvider>
    );
}
