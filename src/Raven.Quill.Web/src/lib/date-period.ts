import { addDays, addMonths, format, getDaysInMonth, isBefore, min, startOfDay, startOfMonth } from "date-fns";

// A hierarchical date period: a whole year, a whole month, or a single day.
// `year` is always set; `month` and `day` narrow the period and are null for
// coarser views, which is what a chart drill-down (year -> months -> days ->
// hours) needs. Periods never extend past today.
export type DatePeriod = {
    year: number;
    /** 1-12, or null when the period is the whole year. */
    month: number | null;
    /** 1-31, or null when no specific day is selected. */
    day: number | null;
};

export function getDefaultDatePeriod(): DatePeriod {
    const now = new Date();
    return { year: now.getFullYear(), month: now.getMonth() + 1, day: null };
}

function toDate(period: DatePeriod): Date {
    return new Date(period.year, (period.month ?? 1) - 1, period.day ?? 1);
}

// Pulls any period back to the latest allowed value: never past today, and the
// day never exceeding the length of the selected month. Passing month 12 /
// day 31 through this is how callers jump to "the latest selectable month/day".
export function clampToToday(period: DatePeriod): DatePeriod {
    const now = new Date();
    const year = Math.min(period.year, now.getFullYear());
    if (period.month === null) return { year, month: null, day: null };

    const month = min([new Date(year, period.month - 1, 1), startOfMonth(now)]);
    if (period.day === null) return { year: month.getFullYear(), month: month.getMonth() + 1, day: null };

    const day = min([addDays(month, Math.min(period.day, getDaysInMonth(month)) - 1), startOfDay(now)]);
    return { year: day.getFullYear(), month: day.getMonth() + 1, day: day.getDate() };
}

export function stepYear(period: DatePeriod, delta: 1 | -1): DatePeriod {
    return clampToToday({ ...period, year: period.year + delta });
}

export function canStepYearUp(period: DatePeriod): boolean {
    return period.year < new Date().getFullYear();
}

export function stepMonth(period: DatePeriod, delta: 1 | -1): DatePeriod {
    if (period.month === null) return period;
    const moved = addMonths(new Date(period.year, period.month - 1, 1), delta);
    return clampToToday({ year: moved.getFullYear(), month: moved.getMonth() + 1, day: period.day });
}

export function canStepMonthUp(period: DatePeriod): boolean {
    return period.month !== null && isBefore(new Date(period.year, period.month - 1, 1), startOfMonth(new Date()));
}

// Day steps roll across month and year boundaries, e.g. Jan 31 -> Feb 1.
export function stepDay(period: DatePeriod, delta: 1 | -1): DatePeriod {
    if (period.month === null || period.day === null) return period;
    const moved = addDays(toDate(period), delta);
    return clampToToday({ year: moved.getFullYear(), month: moved.getMonth() + 1, day: moved.getDate() });
}

export function canStepDayUp(period: DatePeriod): boolean {
    if (period.month === null || period.day === null) return false;
    return isBefore(toDate(period), startOfDay(new Date()));
}

export function formatPeriodLabel(period: DatePeriod): string {
    if (period.month === null) return String(period.year);
    return format(toDate(period), period.day === null ? "MMMM yyyy" : "MMMM d, yyyy");
}

// True while a clicked chart bar can drill the period one level finer: a year
// drills into a month, a month into a day. A day already shows hours, the finest
// bucket, so it is not drillable.
export function canDrillInto(period: DatePeriod): boolean {
    return period.day === null;
}

// Resolves the period a clicked bar drills into from that bucket's date (its
// `from` timestamp or `t` label). Returns null when the value cannot be parsed
// as a date, so callers can ignore the click.
export function drillInto(period: DatePeriod, bucketDate: string): DatePeriod | null {
    const date = new Date(bucketDate);
    if (Number.isNaN(date.getTime())) return null;
    if (period.month === null) return clampToToday({ year: date.getFullYear(), month: date.getMonth() + 1, day: null });
    return clampToToday({ year: date.getFullYear(), month: date.getMonth() + 1, day: date.getDate() });
}

// Query-string shape shared by the period endpoints (stats.usage, stats.appUsage,
// settings.usage, stats.dashboard, stats.conversationStats), which all take the
// period as year/month/day parameters.
export function datePeriodToSearchParams(period: DatePeriod): { year: string; month?: string; day?: string } {
    return {
        year: String(period.year),
        ...(period.month !== null && { month: String(period.month) }),
        ...(period.day !== null && { day: String(period.day) }),
    };
}
