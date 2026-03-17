import { autocomplete } from "../autocompleteUtils";
import { FakeMetadataProvider } from "./FakeMetadataProvider";
import { EmptyMetadataProvider } from "./EmptyMetadataProvider";

const tasks = ["FirstTask", "SecondTask"];

const taskFields = {
    Products: {
        FirstTask: ["Name", "Category"],
        SecondTask: ["Description"],
    },
};

function withTasks() {
    return new FakeMetadataProvider({ aiTasks: taskFields });
}

const embeddingFunctions = ["embedding.text", "embedding.text_i8", "embedding.text_i1"];

describe("ai.task autocomplete", function () {

    describe("combined field+task suggestion", function () {
        test.each(embeddingFunctions)(
            "suggests 'field, ai.task(task)' for each configured path [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(|`,
                    withTasks()
                );

                for (const [taskName, fields] of Object.entries(taskFields.Products)) {
                    for (const field of fields) {
                        const caption = `${field}, ai.task('${taskName}')`;
                        const match = suggestions.find(x => x.caption === caption);
                        expect(match).toBeTruthy();
                    }
                }
            }
        );
        
        test.each(embeddingFunctions)(
            "returns no combined suggestions when no tasks exist [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(|`,
                    new EmptyMetadataProvider()
                );

                const anyAiTask = suggestions.find(x => x.value?.includes("ai.task("));
                expect(anyAiTask).toBeFalsy();
            }
        );

        test.each(embeddingFunctions)(
            "returns no combined suggestions when querying by index (not collection) [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from index 'Product/Rating' where vector.search(${fn}(|`,
                    withTasks()
                );

                const anyAiTask = suggestions.find(x => x.value?.includes("ai.task("));
                expect(anyAiTask).toBeFalsy();
            }
        );

        test.each(embeddingFunctions)(
            "suggests combined field+task [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(|))`,
                    withTasks()
                );

                for (const [taskName, fields] of Object.entries(taskFields.Products)) {
                    for (const field of fields) {
                        const caption = `${field}, ai.task('${taskName}')`;
                        const match = suggestions.find(x => x.caption === caption);
                        expect(match).toBeTruthy();
                    }
                }
            }
        );

        test.each(embeddingFunctions)(
            "returns no suggestions for index query [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from index 'Product/Rating' where vector.search(${fn}(|))`,
                    withTasks()
                );

                const anyAiTask = suggestions.find(x => x.value?.includes("ai.task("));
                expect(anyAiTask).toBeFalsy();
            }
        );
    });

    describe("end of first arg", function () {

        test.each(embeddingFunctions)(
            "suggests ai.task('TaskName') for [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name|`,
                    withTasks()
                );

                for (const task of tasks) {
                    const match = suggestions.find(x => x.caption === `ai.task('${task}')`);
                    expect(match).toBeTruthy();
                }
            }
        );

        test.each(embeddingFunctions)(
            "snippet positions cursor before the comma so the field name can still be edited [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name|`,
                    withTasks()
                );

                for (const task of tasks) {
                    const match = suggestions.find(x => x.caption === `ai.task('${task}')`);
                    expect(match).toBeTruthy();
                    expect(match.snippet).toMatch(/^\$\{0}, ai\.task\(/);
                    expect(match.value).toMatch(/^, ai\.task\(/);
                }
            }
        );

        test.each(embeddingFunctions)(
            "returns no task suggestions when no ai tasks exist [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name|`,
                    new EmptyMetadataProvider()
                );

                const anyAiTask = suggestions.find(x => x.value.includes("ai.task("));
                expect(anyAiTask).toBeFalsy();
            }
        );
    });

    describe("cursor after the comma", function () {

        test.each(embeddingFunctions)(
            "suggests ai.task('TaskName') for each task [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name, |`,
                    withTasks()
                );

                for (const task of tasks) {
                    const match = suggestions.find(x => x.value === `ai.task('${task}')`);
                    expect(match).toBeTruthy();
                }
            }
        );

        test.each(embeddingFunctions)(
            "suggests the generic ai.task() [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name, |`,
                    new EmptyMetadataProvider()
                );

                const genericSnippet = suggestions.find(x => x.value.startsWith("ai.task("));
                expect(genericSnippet).toBeTruthy();
            }
        );

        test.each(embeddingFunctions)(
            "returns no per-task suggestions when no ai tasks exist [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name, |`,
                    new EmptyMetadataProvider()
                );

                const taskSpecific = suggestions.filter(x =>
                    tasks.some(t => x.value === `ai.task('${t}')`)
                );
                expect(taskSpecific).toHaveLength(0);
            }
        );
    });

    describe("cursor inside ai.task('...')", function () {

        test.each(embeddingFunctions)(
            "suggests quoted task names right after the opening [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name, ai.task(|`,
                    withTasks()
                );

                for (const task of tasks) {
                    const match = suggestions.find(x => x.caption === task);
                    expect(match).toBeTruthy();
                    expect(match.value).toBe(`'${task}'`);
                }
            }
        );

        test.each(embeddingFunctions)(
            "suggests quoted task names [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name, ai.task('Open|`,
                    withTasks()
                );

                for (const task of tasks) {
                    const match = suggestions.find(x => x.caption === task);
                    expect(match).toBeTruthy();
                    expect(match.value).toBe(`'${task}'`);
                }
            }
        );

        test.each(embeddingFunctions)(
            "uses double quotes when the user started with a double quote [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name, ai.task("|`,
                    withTasks()
                );

                for (const task of tasks) {
                    const match = suggestions.find(x => x.caption === task);
                    expect(match).toBeTruthy();
                    expect(match.value).toBe(`"${task}"`);
                }
            }
        );

        test.each(embeddingFunctions)(
            "returns no task suggestions when no ai tasks exist [%s]",
            async (fn) => {
                const suggestions = await autocomplete(
                    `from Products where vector.search(${fn}(Name, ai.task(|`,
                    new EmptyMetadataProvider()
                );

                const anyTask = suggestions.find(x =>
                    tasks.some(t => x.caption === t)
                );
                expect(anyTask).toBeFalsy();
            }
        );
    });
});
