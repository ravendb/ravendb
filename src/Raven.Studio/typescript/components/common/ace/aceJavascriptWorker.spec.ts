import * as fs from "fs";
import * as path from "path";
import * as vm from "vm";

/**
 * These tests exercise the actual linter shipped to the browser:
 * wwwroot/Content/ace/worker-javascript.js, produced by the aceBuild project
 * (see aceBuild/README.txt) with the JSHint 2.13.x override from
 * aceBuild/addons/lib/ace/mode/javascript/jshint.js.
 *
 * Why only ES2020 (esversion 11) is supported: patches and other scripts are
 * executed server side by Jint, which understands ES2020 syntax, so the
 * editor must not flag that syntax as invalid. The linter bundled into the
 * worker is JSHint, and the highest esversion JSHint ever implemented is 11
 * (ES2020) - it has no ES2021+ parsing at all. The worker is therefore
 * configured with esversion 11, and newer syntax (e.g. logical assignment
 * operators or numeric separators) is still reported as a syntax error even
 * if the server-side engine could run it. Raising this ceiling requires
 * replacing the JSHint-based worker with a different parser.
 */

interface WorkerAnnotation {
    row: number;
    column: number;
    text: string;
    type: "error" | "warning" | "info";
    raw: string;
}

describe("ace javascript worker (vendored build)", () => {
    let lint: (code: string) => WorkerAnnotation[];

    const noop = (): void => {
        // intentionally empty
    };

    beforeAll(() => {
        const workerPath = path.resolve(__dirname, "../../../../wwwroot/Content/ace/worker-javascript.js");
        const workerSource = fs.readFileSync(workerPath, "utf8");

        // simulate the web worker global scope the script expects
        const sandbox: any = { postMessage: noop, setTimeout, clearTimeout, console };
        vm.createContext(sandbox);
        vm.runInContext(workerSource, sandbox);

        const workerModule = sandbox.require("ace/mode/javascript_worker");

        lint = (code: string) => {
            let annotations: WorkerAnnotation[] = null;
            const sender = {
                on: noop,
                emit: (name: string, data: WorkerAnnotation[]) => {
                    if (name === "annotate") {
                        annotations = data;
                    }
                },
                callback: noop,
            };

            const worker = new workerModule.JavaScriptWorker(sender);
            worker.doc.setValue(code);
            worker.onUpdate();

            return annotations;
        };
    });

    function errorsIn(code: string) {
        return lint(code).filter((x) => x.type === "error");
    }

    describe("accepts ES2020 syntax", () => {
        it.each([
            ["optional chaining", "const oldAmount = $old?.Amount;"],
            ["nullish coalescing", "const amount = $row.amount ?? 0;"],
            ["optional call", "$old?.callback?.();"],
            ["spread with nullish coalescing", "const parts = [...($row.tags ?? [])];"],
            ["template literal", "this.Label = `${$row.name}-suffix`;"],
        ])("%s", (_, code) => {
            expect(errorsIn(code)).toEqual([]);
        });
    });

    describe("rejects ES2021+ syntax", () => {
        it.each([
            ["logical or assignment", "let a = 1; a ||= 2;"],
            ["logical and assignment", "let a = 1; a &&= 2;"],
            ["nullish assignment", "let a = 1; a ??= 2;"],
            ["numeric separator", "const big = 1_000_000;"],
        ])("%s", (_, code) => {
            expect(errorsIn(code)).not.toEqual([]);
        });
    });

    describe("still reports genuine syntax errors", () => {
        it.each([
            ["missing expression", "const x = ;"],
            ["unclosed brace", "if ($row.status === 'VIP') {\n    this.Priority = 'High';"],
        ])("%s", (_, code) => {
            expect(errorsIn(code)).not.toEqual([]);
        });
    });
});
