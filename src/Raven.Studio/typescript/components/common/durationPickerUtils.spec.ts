import {
    DurationUnitSymbols,
    formatDuration,
    getListedValues,
    getVisibleUnits,
    parseDuration,
} from "components/common/durationPickerUtils";

const symbols: DurationUnitSymbols = { days: "d", hours: "h", minutes: "m", seconds: "s" };
const allUnits = getVisibleUnits(true);
const withoutSeconds = getVisibleUnits(false);

describe("durationPickerUtils", () => {
    it("formats only the non-zero units", () => {
        expect(formatDuration(100800 + 300, allUnits, symbols)).toBe("1d 4h 5m");
    });

    it("formats zero as the smallest visible unit", () => {
        expect(formatDuration(0, withoutSeconds, symbols)).toBe("0m");
    });

    it("shows durations past 24 hours with days", () => {
        expect(formatDuration(36 * 3600, withoutSeconds, symbols)).toBe("1d 12h");
    });

    it.each([
        ["1d 4h 5m", 100800 + 300],
        ["1d. 4hr.5m", 100800 + 300],
        ["2 days 30 min", 2 * 86400 + 1800],
        ["90", 90],
        ["1H30M", 5400],
    ])("parses '%s'", (text, expectedSeconds) => {
        expect(parseDuration(text, allUnits, symbols)).toBe(expectedSeconds);
    });

    it("treats a bare number as the smallest visible unit", () => {
        expect(parseDuration("90", withoutSeconds, symbols)).toBe(5400);
    });

    it("returns null for empty text", () => {
        expect(parseDuration("  ", allUnits, symbols)).toBeNull();
    });

    it.each(["abc", "1x", "-5m", "1.5h"])("rejects '%s'", (text) => {
        expect(parseDuration(text, allUnits, symbols)).toBeNaN();
    });

    it("lists up to 365 days and natural limits for the other units", () => {
        expect(getListedValues("days", 0).at(-1)).toBe(365);
        expect(getListedValues("hours", 0).at(-1)).toBe(23);
        expect(getListedValues("seconds", 0).at(-1)).toBe(59);
        expect(getListedValues("minutes", 0).at(-1)).toBe(59);
    });

    it("adds a typed value above 365 after 365 without listing the numbers in between", () => {
        const listedValues = getListedValues("days", 1000);

        expect(listedValues).toHaveLength(367);
        expect(listedValues.slice(-2)).toEqual([365, 1000]);
    });

    it("keeps any typed value without a limit", () => {
        expect(parseDuration("1000d", allUnits, symbols)).toBe(1000 * 86400);
    });
});
