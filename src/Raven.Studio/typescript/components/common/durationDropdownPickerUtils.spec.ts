import {
    DurationUnitSymbols,
    formatDuration,
    getListedValues,
    getVisibleUnits,
    parseDuration,
} from "components/common/durationDropdownPickerUtils";

const symbols: DurationUnitSymbols = { days: "d", hours: "h", minutes: "m", seconds: "s" };
const allUnits = getVisibleUnits(true, true);
const hoursAndMinutes = getVisibleUnits(false, false);

describe("durationDropdownPickerUtils", () => {
    it("formats only the non-zero units", () => {
        expect(formatDuration(100800 + 300, allUnits, symbols)).toBe("1d 4h 5m");
    });

    it("formats zero as the smallest visible unit", () => {
        expect(formatDuration(0, hoursAndMinutes, symbols)).toBe("0m");
    });

    it("rolls days into hours when days are hidden", () => {
        expect(formatDuration(2 * 86400, hoursAndMinutes, symbols)).toBe("48h");
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
        expect(parseDuration("90", hoursAndMinutes, symbols)).toBe(5400);
    });

    it("returns null for empty text", () => {
        expect(parseDuration("  ", allUnits, symbols)).toBeNull();
    });

    it.each(["abc", "1x", "-5m", "1.5h"])("rejects '%s'", (text) => {
        expect(parseDuration(text, allUnits, symbols)).toBeNaN();
    });

    it("lists up to 365 for the largest unit and natural limits below it", () => {
        expect(getListedValues("days", allUnits, 0).at(-1)).toBe(365);
        expect(getListedValues("hours", allUnits, 0).at(-1)).toBe(23);
        expect(getListedValues("hours", hoursAndMinutes, 0).at(-1)).toBe(365);
        expect(getListedValues("minutes", allUnits, 0).at(-1)).toBe(59);
    });

    it("adds a typed value above 365 after 365 without listing the numbers in between", () => {
        const listedValues = getListedValues("days", allUnits, 1000);

        expect(listedValues).toHaveLength(367);
        expect(listedValues.slice(-2)).toEqual([365, 1000]);
    });

    it("keeps any typed value without a limit", () => {
        expect(parseDuration("1000d", allUnits, symbols)).toBe(1000 * 86400);
    });
});
