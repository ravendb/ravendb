type GapNumber = 1 | 2 | 3 | 4 | 5 | 6 | 7;

type GapWithBreakpoint = {
    [breakpoint: string]: GapNumber;
};

export type Gap = GapNumber | GapWithBreakpoint;

export function getGapClasses(gap: Gap): string[] {
    if (!gap) {
        [];
    }

    if (typeof gap === "object") {
        return Object.entries(gap).map(([bp, val]) => `gap-${bp}-${val}`);
    }

    return [`gap-${gap}`];
}
