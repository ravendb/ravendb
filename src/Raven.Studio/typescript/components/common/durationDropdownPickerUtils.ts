export type DurationUnit = "days" | "hours" | "minutes" | "seconds";

export type DurationUnitSymbols = Record<DurationUnit, string>;

type DurationParts = Record<DurationUnit, number>;

export const MAX_LISTED_TOP_UNIT_VALUE = 365;

const allUnits: DurationUnit[] = ["days", "hours", "minutes", "seconds"];

const secondsPerUnit: Record<DurationUnit, number> = {
    days: 24 * 60 * 60,
    hours: 60 * 60,
    minutes: 60,
    seconds: 1,
};

const maxValueBelowUnit: Record<DurationUnit, number> = {
    days: MAX_LISTED_TOP_UNIT_VALUE,
    hours: 23,
    minutes: 59,
    seconds: 59,
};

const unitAliases: Record<DurationUnit, string[]> = {
    days: ["d", "day", "days"],
    hours: ["h", "hr", "hrs", "hour", "hours"],
    minutes: ["m", "min", "mins", "minute", "minutes"],
    seconds: ["s", "sec", "secs", "second", "seconds"],
};

export function getVisibleUnits(showDays: boolean, showSeconds: boolean): DurationUnit[] {
    return allUnits.filter((unit) => (unit === "days" ? showDays : unit === "seconds" ? showSeconds : true));
}

export function splitDuration(totalSeconds: number, units: DurationUnit[]): DurationParts {
    const parts: DurationParts = { days: 0, hours: 0, minutes: 0, seconds: 0 };
    let remainingSeconds = Math.max(0, Math.round(totalSeconds));

    for (const unit of units) {
        parts[unit] = Math.floor(remainingSeconds / secondsPerUnit[unit]);
        remainingSeconds -= parts[unit] * secondsPerUnit[unit];
    }

    return parts;
}

export function joinDuration(parts: DurationParts): number {
    return allUnits.reduce((total, unit) => total + parts[unit] * secondsPerUnit[unit], 0);
}

export function formatDuration(totalSeconds: number, units: DurationUnit[], symbols: DurationUnitSymbols): string {
    if (totalSeconds == null || Number.isNaN(totalSeconds)) {
        return "";
    }

    const parts = splitDuration(totalSeconds, units);
    const nonZeroUnits = units.filter((unit) => parts[unit] > 0);
    const shownUnits = nonZeroUnits.length > 0 ? nonZeroUnits : [units.at(-1)];

    return shownUnits.map((unit) => `${parts[unit]}${symbols[unit]}`).join(" ");
}

export function parseDuration(text: string, units: DurationUnit[], symbols: DurationUnitSymbols): number {
    const normalizedText = text.trim().toLowerCase();
    if (normalizedText === "") {
        return null;
    }

    const tokenPattern = /(\d+)\s*([^\d\s.,]*)/g;
    const leftover = normalizedText.replace(tokenPattern, "");
    if (!/^[\s.,]*$/.test(leftover)) {
        return NaN;
    }

    const tokens = [...normalizedText.matchAll(tokenPattern)];
    const isSingleBareNumber = tokens.length === 1 && tokens[0][2] === "";

    let totalSeconds = 0;
    for (const [, amount, unitText] of tokens) {
        const unit = isSingleBareNumber ? units.at(-1) : findUnit(unitText, symbols);
        if (!unit) {
            return NaN;
        }
        totalSeconds += Number(amount) * secondsPerUnit[unit];
    }

    return totalSeconds;
}

export function getListedValues(unit: DurationUnit, units: DurationUnit[], currentValue: number): number[] {
    const maxValue = unit === units[0] ? MAX_LISTED_TOP_UNIT_VALUE : maxValueBelowUnit[unit];
    const listedValues = Array.from({ length: maxValue + 1 }, (_, value) => value);

    return currentValue > maxValue ? [...listedValues, currentValue] : listedValues;
}

function findUnit(unitText: string, symbols: DurationUnitSymbols): DurationUnit {
    return allUnits.find((unit) => unitAliases[unit].includes(unitText) || symbols[unit].toLowerCase() === unitText);
}
