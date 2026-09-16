import { getErrorHeadline } from "components/utils/common";

describe("common", function () {
    describe("getErrorHeadline", function () {
        it("splits on exception type prefix", () => {
            const result = getErrorHeadline("System.TimeoutException: The operation has timed out.");

            expect(result).toEqual("System.TimeoutException");
        });

        it("returns the whole message when there is no colon", () => {
            const result = getErrorHeadline("Something went wrong");

            expect(result).toEqual("Something went wrong");
        });

        it("does not split on a URI scheme", () => {
            const result = getErrorHeadline("Failed to connect to http://localhost:8080/database");

            expect(result).toEqual("Failed to connect to http://localhost:8080/database");
        });

        it("does not split when the prefix contains a space", () => {
            const result = getErrorHeadline("Error at line 5: unexpected token");

            expect(result).toEqual("Error at line 5: unexpected token");
        });

        it("does not split when there is whitespace before the colon", () => {
            const result = getErrorHeadline("System.InvalidOperationException  : operation is not valid");

            expect(result).toEqual("System.InvalidOperationException  : operation is not valid");
        });
    });
});
