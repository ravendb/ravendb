import genUtils = require("common/generalUtils");

describe("generalUtils", () => {
    describe("isDevBuildNumber", () => {
        it.each([
            [72, true],
            [62, true],
            [72051, false],
            [62057, false],
            [null, false],
            [undefined, false],
        ])("treats build %p as a dev build: %p", (buildNumber, expected) => {
            expect(genUtils.isDevBuildNumber(buildNumber)).toBe(expected);
        });
    });
});
