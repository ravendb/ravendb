import type { ComponentProps, ReactNode } from "react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Text, type TextVariant } from "@/components/typography";
import { formatDate, formatDateTime } from "@/lib/format";
import { cn } from "@/lib/utils";

// The one way to show a moment in time: an exact date.
// `dateVariant="short"` drops the time of day, which a tooltip then carries instead.
// `textVariant="inherit"` leaves the type to whatever the value sits inside (an alert, a badge).
//
// When the visible trigger is something other than the date itself — a badge that names a state,
// with the time behind it — wrap that trigger in <TimestampTooltip>.

type TimestampDateVariant = "full" | "short";
type TimestampTextVariant = TextVariant | "inherit";

export function Timestamp({
    value,
    dateVariant = "full",
    textVariant = "muted",
    fallback = "—",
    className,
}: {
    value: string | null | undefined;
    dateVariant?: TimestampDateVariant;
    textVariant?: TimestampTextVariant;
    fallback?: string;
    className?: string;
}) {
    if (!value) {
        return (
            <TimestampLabel textVariant={textVariant} className={className}>
                {fallback}
            </TimestampLabel>
        );
    }

    if (dateVariant === "full") {
        return (
            <TimestampLabel textVariant={textVariant} className={className}>
                {formatDateTime(value)}
            </TimestampLabel>
        );
    }

    return (
        <TimestampTooltip value={value}>
            <TimestampLabel textVariant={textVariant} className={className}>
                {formatDate(value)}
            </TimestampLabel>
        </TimestampTooltip>
    );
}

export function TimestampTooltip({ value, prefix, children }: { value: string; prefix?: string; children: ReactNode }) {
    const dateTime = formatDateTime(value);

    return (
        <TooltipProvider>
            <Tooltip>
                <TooltipTrigger asChild>{children}</TooltipTrigger>
                <TooltipContent>{prefix ? `${prefix} ${dateTime}` : dateTime}</TooltipContent>
            </Tooltip>
        </TooltipProvider>
    );
}

function TimestampLabel({
    textVariant,
    className,
    children,
    ...props
}: ComponentProps<"span"> & { textVariant: TimestampTextVariant }) {
    if (textVariant === "inherit") {
        return (
            <span className={cn("whitespace-nowrap", className)} {...props}>
                {children}
            </span>
        );
    }

    return (
        <Text as="span" variant={textVariant} className={cn("whitespace-nowrap", className)} {...props}>
            {children}
        </Text>
    );
}
