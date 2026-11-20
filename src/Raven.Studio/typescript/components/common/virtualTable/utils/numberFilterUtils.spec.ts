import { parseNumberCondition } from "./numberFilterUtils";

describe("parseNumberCondition", () => {
    const matching = (filter: string, values: number[], locale = "en-US") =>
        values.filter(parseNumberCondition(filter, locale));

    it("compares with an operator", () => {
        expect(matching("> 1200", [1199, 1200, 1201])).toEqual([1201]);
        expect(matching(">=1200", [1199, 1200, 1201])).toEqual([1200, 1201]);
        expect(matching("< 1.5", [1, 1.5, 2])).toEqual([1]);
        expect(matching("<= 1.5", [1, 1.5, 2])).toEqual([1, 1.5]);
        expect(matching("=100", [99, 100, 101])).toEqual([100]);
    });

    it("matches an inclusive range", () => {
        expect(matching("100..200", [99, 100, 150, 200, 201])).toEqual([100, 150, 200]);
    });

    it("understands size units", () => {
        expect(matching("> 5 MB", [5 * 1024 * 1024, 5 * 1024 * 1024 + 1])).toEqual([5 * 1024 * 1024 + 1]);
        expect(matching("1kb..2KB", [1000, 1024, 2048, 2049])).toEqual([1024, 2048]);
    });

    it("understands numbers formatted like in the table", () => {
        expect(matching("> 10,000", [10_000, 12_500])).toEqual([12_500]);
        expect(matching("1,000.5..2,000", [1000, 1500, 2001])).toEqual([1500]);
        expect(matching("> 10.000", [10_000, 12_500], "de-DE")).toEqual([12_500]);
        expect(matching("< 1,5", [1, 2], "de-DE")).toEqual([1]);
        expect(matching("> 10 000", [10_000, 12_500], "fr-FR")).toEqual([12_500]);
    });

    it("understands a decimal comma in locales that do not group thousands", () => {
        expect(matching("< 1,5", [1, 2], "pl-PL")).toEqual([1]);
        expect(matching("< 1,5", [1, 2], "es-ES")).toEqual([1]);
    });

    it("understands sizes shown with a decimal point in any locale", () => {
        const sizes = [5 * 1024 * 1024, 6 * 1024 * 1024];

        expect(matching("> 5.5 MB", sizes, "de-DE")).toEqual([6 * 1024 * 1024]);
        expect(matching("> 1,023.5 KB", [1023 * 1024, 1024 * 1024], "de-DE")).toEqual([1024 * 1024]);
    });

    it("returns null when the filter is not a number condition", () => {
        expect(parseNumberCondition("1200")).toBeNull();
        expect(parseNumberCondition("> abc")).toBeNull();
        expect(parseNumberCondition("orders/1")).toBeNull();
        expect(parseNumberCondition("")).toBeNull();
    });
});
