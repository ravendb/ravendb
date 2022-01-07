import graphHelper from "common/helpers/graph/graphHelper";

describe("graphHelper", function () {
    describe("shortenLine", function () {
        it("Calculate shorten horizontal line left->right", () => {
            const result = graphHelper.shortenLine(1, 1, 10, 1, 2);

            expect(result.x1)
                .toEqual(3);
            expect(result.y1)
                .toEqual(1);
            expect(result.x2)
                .toEqual(8);
            expect(result.y2)
                .toEqual(1);
        });

        it("Calculate shorten horizontal line right->left", () => {
            const result = graphHelper.shortenLine(10, 1, 1, 1, 2);

            expect(result.x1)
                .toEqual(8);
            expect(result.y1)
                .toEqual(1);
            expect(result.x2)
                .toEqual(3);
            expect(result.y2)
                .toEqual(1);
        });
    });
});
