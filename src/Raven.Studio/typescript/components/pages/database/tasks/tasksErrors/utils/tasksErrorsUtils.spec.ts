import { parseProcessName } from "./tasksErrorsUtils";

describe("parseProcessName", () => {
    test("splits an ETL process name into task and transformation", () => {
        expect(parseProcessName("MyTask/Script1", "Etl")).toEqual(["MyTask", "Script1"]);
    });

    test("an ETL name without a slash has an empty transformation", () => {
        expect(parseProcessName("MyTask", "Etl")).toEqual(["MyTask", ""]);
    });

    test("does not split a CDC Sink name that contains a slash", () => {
        expect(parseProcessName("prod/orders-sink", "CdcSink")).toEqual(["prod/orders-sink", ""]);
    });

    test("does not split an AI task name that contains a slash", () => {
        expect(parseProcessName("my/agent", "Ai")).toEqual(["my/agent", ""]);
    });
});
