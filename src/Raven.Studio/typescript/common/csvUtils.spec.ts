import { toCsv } from "./csvUtils";

describe("csvUtils", () => {
    describe("toCsv", () => {
        it("joins values with commas and rows with CRLF", () => {
            expect(
                toCsv([
                    ["Action", "Duration (ms)"],
                    ["Query", 15],
                ])
            ).toBe("Action,Duration (ms)\r\nQuery,15");
        });

        it("quotes values containing commas, quotes or line breaks", () => {
            expect(toCsv([["a,b", 'say "hi"', "line1\nline2", "cr\rhere"]])).toBe(
                '"a,b","say ""hi""","line1\nline2","cr\rhere"'
            );
        });

        it("writes null and undefined as empty values", () => {
            expect(toCsv([[null, undefined, "x"]])).toBe(",,x");
        });

        it("writes numbers and booleans without locale formatting", () => {
            expect(toCsv([[1234567.5, -5, true, false]])).toBe("1234567.5,-5,true,false");
        });

        it("writes objects as JSON", () => {
            expect(toCsv([[{ Name: "Ann" }, ["a", "b"]]])).toBe('"{""Name"":""Ann""}","[""a"",""b""]"');
        });

        it("prefixes text that a spreadsheet would run as a formula", () => {
            expect(toCsv([["=1+1", "+cmd", "-2+3", "@SUM(A1)", "\tx"]])).toBe("'=1+1,'+cmd,'-2+3,'@SUM(A1),'\tx");
        });

        it("keeps text that only contains formula characters later on", () => {
            expect(toCsv([["a=b", "orders/1-A", "x@y"]])).toBe("a=b,orders/1-A,x@y");
        });
    });
});
