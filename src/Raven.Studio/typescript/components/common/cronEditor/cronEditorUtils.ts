import cronstrue from "cronstrue";
import { createTranslator, i18n } from "common/i18n/i18n";
import { SelectOption } from "components/common/select/Select";

const t = createTranslator("cronEditor");

export type CronPeriod = "minute" | "hour" | "day" | "week" | "month" | "year";

export interface CronBasicSchedule {
    period: CronPeriod;
    minute: number;
    hour: number;
    dayOfWeek: number;
    dayOfMonth: number;
    month: number;
}

const DEFAULT_BASIC_SCHEDULE: Omit<CronBasicSchedule, "period"> = {
    minute: 0,
    hour: 2,
    dayOfWeek: 0,
    dayOfMonth: 1,
    month: 1,
};

const PERIODS: CronPeriod[] = ["minute", "hour", "day", "week", "month", "year"];

const MONTHS = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
] as const;

const DAYS_OF_WEEK = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"] as const;

function getPeriodOptions(): SelectOption<CronPeriod>[] {
    return PERIODS.map((period) => ({ value: period, label: t(`periods.${period}`) }));
}

const MINUTE_STEP = 5;

const STEPPED_MINUTES = range(0, 59).filter((minute) => minute % MINUTE_STEP === 0);

function isCustomMinute(minute: number): boolean {
    return !STEPPED_MINUTES.includes(minute);
}

function getSelectableMinutes(selectedMinute: number): number[] {
    if (selectedMinute == null || !isCustomMinute(selectedMinute)) {
        return STEPPED_MINUTES;
    }

    return [...STEPPED_MINUTES, selectedMinute].sort((a, b) => a - b);
}

function getMinutePastHourOptions(selectedMinute: number): SelectOption<number>[] {
    return getSelectableMinutes(selectedMinute).map((minute) => ({
        value: minute,
        label: minute.toString(),
    }));
}

function getMinuteOptions(selectedMinute: number): SelectOption<number>[] {
    return getSelectableMinutes(selectedMinute).map((minute) => ({
        value: minute,
        label: minute.toString().padStart(2, "0"),
    }));
}

function getHourOptions(): SelectOption<number>[] {
    return range(0, 23).map((hour) => ({
        value: hour,
        label: t(hour < 12 ? "hourAm" : "hourPm", { hour: hour % 12 || 12 }),
    }));
}

function getDayOfWeekOptions(): SelectOption<number>[] {
    return DAYS_OF_WEEK.map((day, idx) => ({ value: idx, label: t(`daysOfWeek.${day}`) }));
}

const DAY_OF_MONTH_OPTIONS: SelectOption<number>[] = range(1, 31).map((day) => ({
    value: day,
    label: day.toString(),
}));

function getMonthOptions(): SelectOption<number>[] {
    return MONTHS.map((month, idx) => ({ value: idx + 1, label: t(`months.${month}`) }));
}

const MINUTE_PATTERN = "[0-5]?\\d";
const HOUR_PATTERN = "(?:[01]?\\d|2[0-3])";
const DAY_OF_MONTH_PATTERN = "(?:0?[1-9]|[12]\\d|3[01])";
const MONTH_PATTERN = "(?:0?[1-9]|1[0-2])";
const DAY_OF_WEEK_PATTERN = "[0-7]";

const BASIC_PATTERNS: { period: CronPeriod; pattern: RegExp }[] = [
    { period: "minute", pattern: /^\* \* \* \* \*$/ },
    { period: "hour", pattern: new RegExp(`^${MINUTE_PATTERN} \\* \\* \\* \\*$`) },
    { period: "day", pattern: new RegExp(`^${MINUTE_PATTERN} ${HOUR_PATTERN} \\* \\* \\*$`) },
    { period: "week", pattern: new RegExp(`^${MINUTE_PATTERN} ${HOUR_PATTERN} \\* \\* ${DAY_OF_WEEK_PATTERN}$`) },
    { period: "month", pattern: new RegExp(`^${MINUTE_PATTERN} ${HOUR_PATTERN} ${DAY_OF_MONTH_PATTERN} \\* \\*$`) },
    {
        period: "year",
        pattern: new RegExp(`^${MINUTE_PATTERN} ${HOUR_PATTERN} ${DAY_OF_MONTH_PATTERN} ${MONTH_PATTERN} \\*$`),
    },
];

function range(from: number, to: number): number[] {
    return Array.from({ length: to - from + 1 }, (_, idx) => from + idx);
}

function normalize(expression: string): string {
    return (expression ?? "").trim().replace(/\s+/g, " ");
}

function parseBasicSchedule(expression: string): CronBasicSchedule | null {
    const normalized = normalize(expression);
    const match = BASIC_PATTERNS.find((x) => x.pattern.test(normalized));
    if (!match) {
        return null;
    }

    const [minute, hour, dayOfMonth, month, dayOfWeek] = normalized.split(" ").map((x) => Number(x));
    const orDefault = (value: number, defaultValue: number) => (Number.isNaN(value) ? defaultValue : value);

    return {
        period: match.period,
        minute: orDefault(minute, DEFAULT_BASIC_SCHEDULE.minute),
        hour: orDefault(hour, DEFAULT_BASIC_SCHEDULE.hour),
        dayOfMonth: orDefault(dayOfMonth, DEFAULT_BASIC_SCHEDULE.dayOfMonth),
        month: orDefault(month, DEFAULT_BASIC_SCHEDULE.month),
        dayOfWeek: orDefault(dayOfWeek, DEFAULT_BASIC_SCHEDULE.dayOfWeek) % 7,
    };
}

function getDefaultBasicSchedule(period: CronPeriod): CronBasicSchedule {
    return { ...DEFAULT_BASIC_SCHEDULE, period };
}

function buildCronExpression({ period, minute, hour, dayOfWeek, dayOfMonth, month }: CronBasicSchedule): string {
    switch (period) {
        case "minute":
            return "* * * * *";
        case "hour":
            return `${minute} * * * *`;
        case "day":
            return `${minute} ${hour} * * *`;
        case "week":
            return `${minute} ${hour} * * ${dayOfWeek}`;
        case "month":
            return `${minute} ${hour} ${dayOfMonth} * *`;
        case "year":
            return `${minute} ${hour} ${dayOfMonth} ${month} *`;
    }
}

const CRONSTRUE_ERROR_PREFIX = /^Error:\s*/;

function getValidationError(expression: string): string | null {
    const normalized = normalize(expression);
    if (!normalized) {
        return null;
    }

    const partsCount = normalized.split(" ").length;
    if (partsCount !== 5) {
        return t("partsCount", { count: partsCount });
    }

    try {
        const description = cronstrue.toString(normalized.toUpperCase());
        return description.includes("undefined") ? t("invalidExpression") : null;
    } catch (error) {
        return typeof error === "string"
            ? t("invalidExpressionDetails", { details: error.replace(CRONSTRUE_ERROR_PREFIX, "") })
            : t("invalidExpression");
    }
}

function getDescription(expression: string): string | null {
    const normalized = normalize(expression);
    if (!normalized || getValidationError(normalized)) {
        return null;
    }

    return cronstrue.toString(normalized.toUpperCase(), { verbose: true, locale: i18n.language });
}

export const cronEditorUtils = {
    getMinutePastHourOptions,
    getMinuteOptions,
    isCustomMinute,
    getHourOptions,
    DAY_OF_MONTH_OPTIONS,
    getPeriodOptions,
    getDayOfWeekOptions,
    getMonthOptions,
    parseBasicSchedule,
    getDefaultBasicSchedule,
    buildCronExpression,
    getValidationError,
    getDescription,
};
