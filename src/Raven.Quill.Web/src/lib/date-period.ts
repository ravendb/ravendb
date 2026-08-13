import { addDays, addMonths, format, getDaysInMonth, isBefore, max, min, startOfDay, startOfMonth } from "date-fns";

// A hierarchical date period: a whole year, a whole month, or a single day.
// `year` is always set; `month` and `day` narrow the period and are null for
// coarser views, which is what a chart drill-down (year -> months -> days ->
// hours) needs. Periods never extend past today, and never before the setup's first
// day once callers pass that bound to the clamping helpers below.
export type DatePeriod = {
    year: number;
    /** 1-12, or null when the period is the whole year. */
    month: number | null;
    /** 1-31, or null when no specific day is selected. */
    day: number | null;
};

// The lower bound a view can select, read from a server timestamp. Returns undefined for
// the values a server reports when it has none (missing, unparsable, the 0001-01-01
// default, or a clock-skewed future date), because a bogus bound would block more of the
// calendar than it should.
export function parseStartDate(value: string | null | undefined): Date | undefined {
    if (!value) return undefined;
    const startDate = new Date(value);
    if (Number.isNaN(startDate.getTime()) || startDate.getFullYear() < 2000 || startDate > new Date()) {
        return undefined;
    }
    return startOfDay(startDate);
}

export function getDefaultDatePeriod(): DatePeriod {
    const now = new Date();
    return { year: now.getFullYear(), month: now.getMonth() + 1, day: null };
}

function toDate(period: DatePeriod): Date {
    return new Date(period.year, (period.month ?? 1) - 1, period.day ?? 1);
}

function clampDate(date: Date, earliest: Date | undefined, latest: Date): Date {
    const withinLatest = min([date, latest]);
    return earliest ? max([withinLatest, earliest]) : withinLatest;
}

// Pulls any period into the selectable range: never past today, never before
// `earliest` when one is given, and the day never exceeding the length of the
// selected month. Passing month 12 / day 31 through this is how callers jump to
// "the latest selectable month/day".
export function clampPeriod(period: DatePeriod, earliest?: Date): DatePeriod {
    const now = new Date();
    if (period.month === null) {
        const year = Math.min(period.year, now.getFullYear());
        return { year: earliest ? Math.max(year, earliest.getFullYear()) : year, month: null, day: null };
    }

    const month = clampDate(
        new Date(period.year, period.month - 1, 1),
        earliest && startOfMonth(earliest),
        startOfMonth(now),
    );
    if (period.day === null) return { year: month.getFullYear(), month: month.getMonth() + 1, day: null };

    const dayInMonth = addDays(month, Math.min(period.day, getDaysInMonth(month)) - 1);
    const day = clampDate(dayInMonth, earliest && startOfDay(earliest), startOfDay(now));
    return { year: day.getFullYear(), month: day.getMonth() + 1, day: day.getDate() };
}

export function isSameDatePeriod(a: DatePeriod, b: DatePeriod): boolean {
    return a.year === b.year && a.month === b.month && a.day === b.day;
}

export function stepYear(period: DatePeriod, delta: 1 | -1, earliest?: Date): DatePeriod {
    return clampPeriod({ ...period, year: period.year + delta }, earliest);
}

export function canStepYearUp(period: DatePeriod): boolean {
    return period.year < new Date().getFullYear();
}

export function stepMonth(period: DatePeriod, delta: 1 | -1, earliest?: Date): DatePeriod {
    if (period.month === null) return period;
    const moved = addMonths(new Date(period.year, period.month - 1, 1), delta);
    return clampPeriod({ year: moved.getFullYear(), month: moved.getMonth() + 1, day: period.day }, earliest);
}

export function canStepMonthUp(period: DatePeriod): boolean {
    return period.month !== null && isBefore(new Date(period.year, period.month - 1, 1), startOfMonth(new Date()));
}

// Day steps roll across month and year boundaries, e.g. Jan 31 -> Feb 1.
export function stepDay(period: DatePeriod, delta: 1 | -1, earliest?: Date): DatePeriod {
    if (period.month === null || period.day === null) return period;
    const moved = addDays(toDate(period), delta);
    return clampPeriod({ year: moved.getFullYear(), month: moved.getMonth() + 1, day: moved.getDate() }, earliest);
}

export function canStepDayUp(period: DatePeriod): boolean {
    if (period.month === null || period.day === null) return false;
    return isBefore(toDate(period), startOfDay(new Date()));
}

// The granularity a period selects: a whole year, one month, or one day.
export function datePeriodUnit(period: DatePeriod): "day" | "month" | "year" {
    if (period.day !== null) return "day";
    if (period.month !== null) return "month";
    return "year";
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
export function drillInto(period: DatePeriod, bucketDate: string, earliest?: Date): DatePeriod | null {
    const date = new Date(bucketDate);
    if (Number.isNaN(date.getTime())) return null;
    const day = period.month === null ? null : date.getDate();
    return clampPeriod({ year: date.getFullYear(), month: date.getMonth() + 1, day }, earliest);
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
