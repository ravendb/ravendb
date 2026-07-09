export type MonthSelection = { year: number; month: number };

export function getCurrentMonth(): MonthSelection {
    const now = new Date();
    return { year: now.getFullYear(), month: now.getMonth() + 1 };
}

export function formatMonthLabel({ year, month }: MonthSelection): string {
    return new Date(year, month - 1).toLocaleString("en-US", { month: "long", year: "numeric" });
}
