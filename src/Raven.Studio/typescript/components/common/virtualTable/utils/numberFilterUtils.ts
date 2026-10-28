export type NumberCondition = (value: number) => boolean;

const sizeUnitMultipliers: Record<string, number> = {
    b: 1,
    kb: 1024,
    mb: 1024 ** 2,
    gb: 1024 ** 3,
    tb: 1024 ** 4,
};

const numberPattern = /^(-?\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb)?$/i;
const comparisonPattern = /^(>=|<=|>|<|=)\s*(.+)$/;
const rangePattern = /^(.+?)\s*\.\.\s*(.+)$/;
const separatorPattern = /[.,]/g;
const digitsInGroup = 3;

export function parseNumberCondition(filter: string, locale?: string): NumberCondition | null {
    const trimmedFilter = filter.trim();
    const localeDecimalSeparator = getDecimalSeparator(locale);

    const comparison = comparisonPattern.exec(trimmedFilter);
    if (comparison) {
        const [, operator, operand] = comparison;
        const limit = parseNumber(operand, localeDecimalSeparator);
        return limit == null ? null : getComparison(operator, limit);
    }

    const range = rangePattern.exec(trimmedFilter);
    if (range) {
        const from = parseNumber(range[1], localeDecimalSeparator);
        const to = parseNumber(range[2], localeDecimalSeparator);
        return from == null || to == null ? null : (value) => value >= from && value <= to;
    }

    return null;
}

function getDecimalSeparator(locale?: string): string {
    const parts = new Intl.NumberFormat(locale).formatToParts(1.5);
    return parts.find((x) => x.type === "decimal")?.value ?? ".";
}

function parseNumber(text: string, localeDecimalSeparator: string): number | null {
    const compactText = text.trim().replace(/(\d)\s+(?=\d)/g, "$1");
    const decimalSeparatorIndex = findDecimalSeparatorIndex(compactText, localeDecimalSeparator);
    const invariantText = compactText.replace(separatorPattern, (_, index: number) =>
        index === decimalSeparatorIndex ? "." : ""
    );

    const match = numberPattern.exec(invariantText);
    if (!match) {
        return null;
    }

    const [, number, unit] = match;
    return Number(number) * (unit ? sizeUnitMultipliers[unit.toLowerCase()] : 1);
}

function findDecimalSeparatorIndex(text: string, localeDecimalSeparator: string): number {
    const separators = [...text.matchAll(separatorPattern)];
    const lastSeparator = separators.at(-1);

    if (!lastSeparator) {
        return -1;
    }

    const hasMixedSeparators = separators.some((x) => x[0] !== lastSeparator[0]);
    if (hasMixedSeparators) {
        return lastSeparator.index;
    }

    if (separators.length > 1) {
        return -1;
    }

    const digitsAfterSeparator = /^\d*/.exec(text.slice(lastSeparator.index + 1))[0].length;
    const isGroupSeparator = digitsAfterSeparator === digitsInGroup && lastSeparator[0] !== localeDecimalSeparator;

    return isGroupSeparator ? -1 : lastSeparator.index;
}

function getComparison(operator: string, limit: number): NumberCondition {
    switch (operator) {
        case ">":
            return (value) => value > limit;
        case ">=":
            return (value) => value >= limit;
        case "<":
            return (value) => value < limit;
        case "<=":
            return (value) => value <= limit;
        default:
            return (value) => value === limit;
    }
}
