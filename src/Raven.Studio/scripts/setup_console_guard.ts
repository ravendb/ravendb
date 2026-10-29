import { format } from "node:util";

const guardedMethods = ["log", "info", "warn", "error", "debug"] as const;
const originalMethods = guardedMethods.map((method) => console[method]);
const unexpectedCalls: string[] = [];

guardedMethods.forEach((method) => {
    console[method] = (...args: unknown[]) => {
        const callerStack = new Error().stack.split("\n").slice(2).join("\n");
        unexpectedCalls.push(`console.${method}: ${format(...args)}\n${callerStack}`);
    };
});

function throwOnUnexpectedConsoleCalls() {
    if (unexpectedCalls.length === 0) {
        return;
    }

    throw new Error(
        "Tests must not print to the console. Fix the cause, or mock the console method in the test if the output is expected.\n\n" +
            unexpectedCalls.splice(0).join("\n\n")
    );
}

afterEach(throwOnUnexpectedConsoleCalls);

afterAll(() => {
    guardedMethods.forEach((method, index) => {
        console[method] = originalMethods[index];
    });
    throwOnUnexpectedConsoleCalls();
});
