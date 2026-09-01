import type { ComponentProps, ReactNode } from "react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Text, type TextVariant } from "@/components/typography";
import { formatDate, formatDateTime, formatRelativeTime } from "@/lib/format";
import { cn } from "@/lib/utils";

// The one way to show a moment in time: an exact date, with the relative reading in a tooltip.
// `dateVariant="short"` drops the time of day, which the tooltip then carries instead.
// `textVariant="inherit"` leaves the type to whatever the value sits inside (an alert, a badge).
//
// When the visible trigger is something other than the date itself — a badge that names a state,
// with the time behind it — wrap that trigger in <TimestampTooltip> so the tooltip reads the same.

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

    return (
        <TimestampTooltip value={value} withDateTime={dateVariant === "short"}>
            <TimestampLabel textVariant={textVariant} className={className}>
                {dateVariant === "short" ? formatDate(value) : formatDateTime(value)}
            </TimestampLabel>
        </TimestampTooltip>
    );
}

export function TimestampTooltip({
    value,
    prefix,
    withDateTime = true,
    children,
}: {
    value: string;
    prefix?: string;
    withDateTime?: boolean;
    children: ReactNode;
}) {
    return (
        <TooltipProvider>
            <Tooltip>
                <TooltipTrigger asChild>{children}</TooltipTrigger>
                <TooltipContent className="flex-col items-start gap-0.5">
                    <TimestampTooltipBody value={value} prefix={prefix} withDateTime={withDateTime} />
                </TooltipContent>
            </Tooltip>
        </TooltipProvider>
    );
}

// Its own component so the age is read when the tooltip opens rather than when the trigger rendered
function TimestampTooltipBody({
    value,
    prefix,
    withDateTime,
}: {
    value: string;
    prefix?: string;
    withDateTime: boolean;
}) {
    const relative = formatRelativeTime(value);

    return (
        <>
            <span>{prefix ? `${prefix} ${relative}` : relative}</span>
            {withDateTime && <span>{formatDateTime(value)}</span>}
        </>
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
