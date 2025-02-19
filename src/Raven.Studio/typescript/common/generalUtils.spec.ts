import genUtils from "common/generalUtils";

describe("generalUtils", () => {
    describe("compareSets", () => {
        it("should return true if the sets are equal", () => {
            expect(genUtils.compareSets([], [])).toBe(true);
            expect(genUtils.compareSets([1, 2, 3], [1, 2, 3])).toBe(true);
            expect(genUtils.compareSets([3, 2, 1], [1, 2, 3])).toBe(true);
        });

        it("should return false if the sets are not equal", () => {
            expect(genUtils.compareSets([], null)).toBe(false);
            expect(genUtils.compareSets([], undefined)).toBe(false);
            expect(genUtils.compareSets([1, 1], [1, 2])).toBe(false);
            expect(genUtils.compareSets([1, 2], [1, 2, 3])).toBe(false);
            expect(genUtils.compareSets([1, 2, 3], [1, 2])).toBe(false);
        });
    });
});
