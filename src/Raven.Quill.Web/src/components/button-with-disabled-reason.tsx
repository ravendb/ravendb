import type { ComponentProps } from "react";
import { Button } from "@/components/shadcn/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { cn } from "@/lib/utils";

type ButtonWithDisabledReasonProps = ComponentProps<typeof Button> & {
    /** Why the button cannot be used right now. An empty string leaves it enabled. */
    disabledReason: string;
};

export function ButtonWithDisabledReason({
    disabledReason,
    className,
    onClick,
    ...buttonProps
}: ButtonWithDisabledReasonProps) {
    const isDisabled = Boolean(disabledReason);

    return (
        <TooltipProvider>
            <Tooltip>
                {/* aria-disabled, not disabled, so the button stays focusable and its tooltip
                    reaches keyboard and screen reader users as the button's description. */}
                <TooltipTrigger asChild>
                    <Button
                        {...buttonProps}
                        aria-disabled={isDisabled ? true : undefined}
                        className={cn(isDisabled && "cursor-not-allowed opacity-50", className)}
                        onClick={isDisabled ? undefined : onClick}
                    />
                </TooltipTrigger>
                {isDisabled && <TooltipContent>{disabledReason}</TooltipContent>}
            </Tooltip>
        </TooltipProvider>
    );
}
