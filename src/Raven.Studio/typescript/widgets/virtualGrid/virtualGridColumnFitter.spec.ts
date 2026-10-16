import { fitColumnWidths } from "./virtualGridColumnFitter";

describe("virtualGridColumnFitter", () => {
    describe("fitColumnWidths", () => {
        it("keeps short columns at their content width and gives the rest to the long-text column", () => {
            const widths = fitColumnWidths(
                [
                    { header: 60, content: 110 },
                    { header: 80, content: 190 },
                    { header: 70, content: 900 },
                ],
                1000
            );

            expect(widths).toEqual([110, 190, 700]);
        });

        it("never makes a column narrower than its header", () => {
            const widths = fitColumnWidths(
                [
                    { header: 150, content: 60 },
                    { header: 60, content: 600 },
                ],
                700
            );

            expect(widths).toEqual([150, 550]);
        });

        it("shares the space between long-text columns in proportion to their content", () => {
            const widths = fitColumnWidths(
                [
                    { header: 60, content: 300 },
                    { header: 60, content: 900 },
                ],
                720
            );

            expect(widths).toEqual([240, 480]);
        });

        it("gives a long-text column only what it needs and passes the rest on", () => {
            const widths = fitColumnWidths(
                [
                    { header: 60, content: 250 },
                    { header: 60, content: 900 },
                ],
                1200
            );

            expect(widths).toEqual([250, 950]);
        });

        it("fills the available width when every value fits", () => {
            const widths = fitColumnWidths(
                [
                    { header: 50, content: 80 },
                    { header: 50, content: 100 },
                ],
                400
            );

            expect(widths).toEqual([80, 320]);
        });

        it("gives the leftover space to the last column when content widths tie", () => {
            const widths = fitColumnWidths(
                [
                    { header: 50, content: 100 },
                    { header: 50, content: 100 },
                ],
                300
            );

            expect(widths).toEqual([100, 200]);
        });

        it("falls back to header widths and a minimum for long text when the container is too narrow", () => {
            const widths = fitColumnWidths(
                [
                    { header: 100, content: 40 },
                    { header: 90, content: 500 },
                ],
                150
            );

            expect(widths).toEqual([100, 120]);
        });
    });
});
