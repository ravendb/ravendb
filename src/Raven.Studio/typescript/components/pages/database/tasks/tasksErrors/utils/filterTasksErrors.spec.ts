import { TaskWithErrors, TasksFiltersState } from "./tasksErrorsUtils";
import { filterTasksWithErrors } from "components/pages/database/tasks/tasksErrors/utils/filterTasksErrors";

function makeTask(overrides: Partial<TaskWithErrors> = {}): TaskWithErrors {
    return {
        etlName: "MyTask",
        etlType: "Raven",
        category: "Etl",
        transformations: [
            {
                transformationName: "Script1",
                itemErrors: [
                    {
                        TaskName: "MyTask/Script1",
                        nodeTag: "A",
                        shardNumber: null,
                        DocumentId: "doc/1",
                        Error: "err",
                        Step: "Load",
                        CreatedAt: "2024-01-01",
                    },
                    {
                        TaskName: "MyTask/Script1",
                        nodeTag: "B",
                        shardNumber: null,
                        DocumentId: "doc/2",
                        Error: "err",
                        Step: "Load",
                        CreatedAt: "2024-01-01",
                    },
                ],
                processErrors: [],
            },
        ],
        ...overrides,
    };
}

function makeCdcTask(overrides: Partial<TaskWithErrors> = {}): TaskWithErrors {
    return {
        etlName: "MyCdcSink",
        etlType: undefined,
        category: "CdcSink",
        transformations: [
            {
                transformationName: "",
                itemErrors: [
                    {
                        TaskName: "MyCdcSink",
                        nodeTag: "A",
                        shardNumber: null,
                        DocumentId: "doc/1",
                        Error: "err",
                        Step: "Load",
                        CreatedAt: "2024-01-01",
                    },
                ],
                processErrors: [],
            },
        ],
        ...overrides,
    };
}

const emptyFilters: TasksFiltersState = {
    searchText: "",
    nodeTags: [],
    shardNumbers: [],
    healthStatuses: [],
    taskTypes: [],
};

describe("filterTasksWithErrors - node filter", () => {
    test("returns all errors when no node filter is set", () => {
        const tasks = [makeTask()];
        const result = filterTasksWithErrors(tasks, [], emptyFilters);

        expect(result[0].transformations[0].itemErrors).toHaveLength(2);
    });

    test("filters itemErrors to only matching node", () => {
        const tasks = [makeTask()];
        const result = filterTasksWithErrors(tasks, [], { ...emptyFilters, nodeTags: ["A"] });

        expect(result[0].transformations[0].itemErrors).toHaveLength(1);
        expect(result[0].transformations[0].itemErrors[0].nodeTag).toBe("A");
    });

    test("hides transformation when no errors match node filter", () => {
        const tasks = [makeTask()];
        const result = filterTasksWithErrors(tasks, [], { ...emptyFilters, nodeTags: ["C"] });

        expect(result).toHaveLength(0);
    });

    test("filters processErrors to only matching node", () => {
        const tasks = [
            makeTask({
                transformations: [
                    {
                        transformationName: "Script1",
                        itemErrors: [],
                        processErrors: [
                            {
                                TaskName: "MyTask/Script1",
                                AffectedDocumentsCount: 1,
                                nodeTag: "A",
                                shardNumber: null,
                                Error: "err",
                                Step: "Load",
                                CreatedAt: "2024-01-01",
                            },
                            {
                                TaskName: "MyTask/Script1",
                                AffectedDocumentsCount: 1,
                                nodeTag: "B",
                                shardNumber: null,
                                Error: "err",
                                Step: "Load",
                                CreatedAt: "2024-01-01",
                            },
                        ],
                    },
                ],
            }),
        ];
        const result = filterTasksWithErrors(tasks, [], { ...emptyFilters, nodeTags: ["B"] });

        expect(result[0].transformations[0].processErrors).toHaveLength(1);
        expect(result[0].transformations[0].processErrors[0].nodeTag).toBe("B");
    });
});

describe("filterTasksWithErrors - shard filter", () => {
    test("filters itemErrors to only matching shard", () => {
        const tasks = [
            makeTask({
                transformations: [
                    {
                        transformationName: "Script1",
                        itemErrors: [
                            {
                                TaskName: "MyTask/Script1",
                                nodeTag: "A",
                                shardNumber: 0,
                                DocumentId: "doc/1",
                                Error: "err",
                                Step: "Load",
                                CreatedAt: "2024-01-01",
                            },
                            {
                                TaskName: "MyTask/Script1",
                                nodeTag: "A",
                                shardNumber: 1,
                                DocumentId: "doc/2",
                                Error: "err",
                                Step: "Load",
                                CreatedAt: "2024-01-01",
                            },
                        ],
                        processErrors: [],
                    },
                ],
            }),
        ];
        const result = filterTasksWithErrors(tasks, [], { ...emptyFilters, shardNumbers: ["1"] });

        expect(result[0].transformations[0].itemErrors).toHaveLength(1);
        expect(result[0].transformations[0].itemErrors[0].shardNumber).toBe(1);
    });
});

describe("filterTasksWithErrors - task type filter", () => {
    test("returns all tasks when no task-type filter is set", () => {
        const result = filterTasksWithErrors([makeTask(), makeCdcTask()], [], emptyFilters);
        expect(result).toHaveLength(2);
    });

    test("shows CDC Sink errors when filtering by the CDC Sink task type", () => {
        const result = filterTasksWithErrors([makeTask(), makeCdcTask()], [], {
            ...emptyFilters,
            taskTypes: ["CdcSink"],
        });

        expect(result).toHaveLength(1);
        expect(result[0].category).toBe("CdcSink");
    });

    test("hides CDC Sink errors when filtering by an ETL task type only", () => {
        const result = filterTasksWithErrors([makeTask(), makeCdcTask()], [], {
            ...emptyFilters,
            taskTypes: ["RavenEtl"],
        });

        expect(result).toHaveLength(1);
        expect(result[0].category).toBe("Etl");
    });
});
