import type { ComponentProps, ReactNode } from "react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Text, type TextVariant } from "@/components/typography";
import { formatDate, formatDateTime, formatDateTimeUtc, formatDateUtc } from "@/lib/format";
import { cn } from "@/lib/utils";

// The one way to show a moment in time: an exact date.
// `dateVariant="short"` drops the time of day, which a tooltip then carries instead.
// `textVariant="inherit"` leaves the type to whatever the value sits inside (an alert, a badge).
// `timeZone="utc"` is for dates filtered by a <DatePeriodPicker>, so they agree with the UTC period.
//
// When the visible trigger is something other than the date itself — a badge that names a state,
// with the time behind it — wrap that trigger in <TimestampTooltip>.

type TimestampDateVariant = "full" | "short";
type TimestampTextVariant = TextVariant | "inherit";
type TimestampTimeZone = "local" | "utc";

function formatTimestampDate(value: string, timeZone: TimestampTimeZone) {
    return timeZone === "utc" ? formatDateUtc(value) : formatDate(value);
}

function formatTimestampDateTime(value: string, timeZone: TimestampTimeZone) {
    return timeZone === "utc" ? formatDateTimeUtc(value) : formatDateTime(value);
}

export function Timestamp({
    value,
    dateVariant = "full",
    textVariant = "muted",
    timeZone = "local",
    fallback = "—",
    className,
}: {
    value: string | null | undefined;
    dateVariant?: TimestampDateVariant;
    textVariant?: TimestampTextVariant;
    timeZone?: TimestampTimeZone;
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
                {formatTimestampDateTime(value, timeZone)}
            </TimestampLabel>
        );
    }

    return (
        <TimestampTooltip value={value} timeZone={timeZone}>
            <TimestampLabel textVariant={textVariant} className={className}>
                {formatTimestampDate(value, timeZone)}
            </TimestampLabel>
        </TimestampTooltip>
    );
}

export function TimestampTooltip({
    value,
    prefix,
    timeZone = "local",
    children,
}: {
    value: string;
    prefix?: string;
    timeZone?: TimestampTimeZone;
    children: ReactNode;
}) {
    const dateTime = formatTimestampDateTime(value, timeZone);

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
