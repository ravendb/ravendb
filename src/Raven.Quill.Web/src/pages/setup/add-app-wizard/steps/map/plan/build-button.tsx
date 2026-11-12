import { Hammer } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { cn } from "@/lib/utils";

export function BuildButton({
    label,
    blockReason,
    onBuild,
    isPrimary = false,
}: {
    label: string;
    blockReason: string;
    onBuild: () => void;
    isPrimary?: boolean;
}) {
    const button = (
        <Button
            variant={isPrimary ? "default" : "outline"}
            size="sm"
            aria-disabled={blockReason ? true : undefined}
            className={cn(blockReason && "cursor-not-allowed opacity-50")}
            onClick={() => !blockReason && onBuild()}
        >
            <Hammer data-icon="inline-start" />
            {label}
        </Button>
    );

    if (!blockReason) {
        return button;
    }

    return (
        <Tooltip>
            <TooltipTrigger asChild>{button}</TooltipTrigger>
            <TooltipContent>{blockReason}</TooltipContent>
        </Tooltip>
    );
}
