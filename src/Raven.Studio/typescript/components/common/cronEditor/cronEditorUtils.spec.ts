import { changeLanguage } from "common/i18n/i18n";
import { cronEditorUtils } from "./cronEditorUtils";

describe("cronEditorUtils", () => {
    afterEach(async () => {
        await changeLanguage("en");
    });

    it.each([
        ["* * * * *", "minute"],
        ["15 * * * *", "hour"],
        ["0 2 * * *", "day"],
        ["0 2 * * 0", "week"],
        ["0 2 1 * *", "month"],
        ["30 4 1 6 *", "year"],
    ])("reads %s as a basic %s schedule and builds the same expression", (expression, period) => {
        const schedule = cronEditorUtils.parseBasicSchedule(expression);

        expect(schedule.period).toBe(period);
        expect(cronEditorUtils.buildCronExpression(schedule)).toBe(expression);
    });

    it("treats an expression that the basic editor cannot show as advanced", () => {
        expect(cronEditorUtils.parseBasicSchedule("0 */6 * * 1-5")).toBeNull();
    });

    it.each(["99 * * * *", "0 24 * * *", "0 2 * * 9", "0 2 32 * *", "0 2 1 13 *"])(
        "treats %s with an out of range value as advanced",
        (expression) => {
            expect(cronEditorUtils.parseBasicSchedule(expression)).toBeNull();
        }
    );

    it("reads Sunday written as 7 as the first day of the week", () => {
        expect(cronEditorUtils.parseBasicSchedule("0 2 * * 7").dayOfWeek).toBe(0);
    });

    it("offers minutes in 5 minute steps", () => {
        const minutes = cronEditorUtils.getMinutePastHourOptions(15).map((x) => x.value);

        expect(minutes).toEqual([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    });

    it("adds a minute between the steps as a custom option in its sorted place", () => {
        const minutes = cronEditorUtils.getMinuteOptions(2).map((x) => x.label);

        expect(minutes.slice(0, 3)).toEqual(["00", "02", "05"]);
        expect(minutes).toHaveLength(13);
        expect(cronEditorUtils.isCustomMinute(2)).toBe(true);
    });

    it("describes a schedule in the language of the Studio", async () => {
        expect(cronEditorUtils.getDescription("0 2 * * *")).toBe("At 02:00 AM, every day");

        await changeLanguage("pl");
        expect(cronEditorUtils.getDescription("0 2 * * *")).toBe("O 02:00, co dzień");
    });

    it("rejects an expression that does not have 5 parts", () => {
        expect(cronEditorUtils.getValidationError("0 2 * *")).toBe(
            "Expression has 4 parts. Exactly 5 parts are required."
        );
    });

    it("rejects an expression with an invalid value and says which part is wrong", async () => {
        expect(cronEditorUtils.getValidationError("0 25 * * *")).toBe(
            "Invalid cron expression: hours part must be >= 0 and <= 23"
        );

        await changeLanguage("pl");
        expect(cronEditorUtils.getValidationError("0 25 * * *")).toBe(
            "Nieprawidłowe wyrażenie cron: hours part must be >= 0 and <= 23"
        );
    });
});
