import { compareSets, isBoolean, range, isEmpty } from "./typeUtils";

describe("typeUtils", () => {
    describe("isBoolean", () => {
        it("should return true for booleans", () => {
            expect(isBoolean(true)).toBe(true);
            expect(isBoolean(false)).toBe(true);
        });

        it("should return false for non-booleans", () => {
            expect(isBoolean(null)).toBe(false);
            expect(isBoolean(undefined)).toBe(false);
            expect(isBoolean("foo")).toBe(false);
            expect(isBoolean(0)).toBe(false);
            expect(isBoolean(-1)).toBe(false);
            expect(isBoolean(1)).toBe(false);
            expect(isBoolean({})).toBe(false);
            expect(isBoolean([])).toBe(false);
        });
    });

    describe("range", () => {
        it("should return an array with the correct values", () => {
            // increment
            expect(range(-1, 2)).toEqual([-1, 0, 1]);
            expect(range(0, 0)).toEqual([]);
            expect(range(0, 1)).toEqual([0]);
            expect(range(0, 5)).toEqual([0, 1, 2, 3, 4]);
            expect(range(0, 5, 2)).toEqual([0, 2, 4]);
            expect(range(0, 5, 3)).toEqual([0, 3]);

            // decrement
            expect(range(5, 0)).toEqual([5, 4, 3, 2, 1]);
            expect(range(5, 0, -2)).toEqual([5, 3, 1]);
            expect(range(5, 0, -3)).toEqual([5, 2]);
        });
    });

    describe("compareSets", () => {
        it("should return true if the sets are equal", () => {
            expect(compareSets([], [])).toBe(true);
            expect(compareSets([1, 2, 3], [1, 2, 3])).toBe(true);
            expect(compareSets([3, 2, 1], [1, 2, 3])).toBe(true);
        });

        it("should return false if the sets are not equal", () => {
            expect(compareSets([], null)).toBe(false);
            expect(compareSets([], undefined)).toBe(false);
            expect(compareSets([1, 1], [1, 2])).toBe(false);
            expect(compareSets([1, 2], [1, 2, 3])).toBe(false);
            expect(compareSets([1, 2, 3], [1, 2])).toBe(false);
        });
    });
    
    describe("isEmpty", () => {
        it("should return true for empty values", () => {
            expect(isEmpty(undefined)).toBe(true);
            expect(isEmpty(null)).toBe(true);
            expect(isEmpty("")).toBe(true);
            expect(isEmpty([])).toBe(true);
            expect(isEmpty({})).toBe(true);
            expect(isEmpty(0)).toBe(true);
            expect(isEmpty(false)).toBe(true);
            expect(isEmpty(true)).toBe(true);
        });
        
        it("should return false for non-empty values", () => {
            expect(isEmpty("hello")).toBe(false);
            expect(isEmpty(" ")).toBe(false);
            expect(isEmpty(15)).toBe(false);
            expect(isEmpty([0, false, "test"])).toBe(false);
            expect(isEmpty({ key: "value" })).toBe(false);
        });
    });
});
