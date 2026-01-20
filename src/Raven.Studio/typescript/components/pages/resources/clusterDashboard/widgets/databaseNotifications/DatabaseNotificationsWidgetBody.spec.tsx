import {
    mapToRowItems,
    type DatabaseNotificationsWidgetTableRowItem as RowItem,
} from "components/pages/resources/clusterDashboard/widgets/databaseNotifications/DatabaseNotificationsWidgetBody";
import databaseNotificationsItem from "models/resources/widgets/databaseNotificationsItem";

describe("DatabaseNotificationsWidgetBody", () => {
    describe("mapToRowItems", () => {
        it("can map single db on single node w/o alerts or performance hints", () => {
            const flatItems = [
                new databaseNotificationsItem("A", {
                    DatabaseName: "db1",
                    AlertsCount: 0,
                    Alerts: [],
                    PerformanceHintsCount: 0,
                    PerformanceHints: [],
                }),
            ];

            const result = mapToRowItems(flatItems);

            expect(result).toEqual([
                {
                    dbName: "db1",
                    nodeTag: "A",
                    alertsCount: 0,
                    perfHintsCount: 0,
                    subRows: [],
                },
            ] satisfies RowItem[]);
        });

        it("can map single db on single node w/ alerts and performance hints", () => {
            const flatItems = [
                new databaseNotificationsItem("A", {
                    DatabaseName: "db1",
                    AlertsCount: 2,
                    Alerts: [
                        { Count: 2, Reason: "QueueSink_Error", PrettifiedReason: "Queue Sink: Invalid configuration" },
                    ],
                    PerformanceHintsCount: 2,
                    PerformanceHints: [
                        { Count: 1, Reason: "Indexing", PrettifiedReason: "Indexing: Definition issues" },
                        { Count: 1, Reason: "SlowIO", PrettifiedReason: "Storage: An extremely slow write to disk" },
                    ],
                }),
            ];

            const result = mapToRowItems(flatItems);

            expect(result).toEqual([
                {
                    dbName: "db1",
                    nodeTag: "A",
                    alertsCount: 2,
                    perfHintsCount: 2,
                    subRows: [
                        {
                            alertsCount: 2,
                            alertPrettifiedReason: "Queue Sink: Invalid configuration",
                            perfHintsCount: 1,
                            perfHintPrettifiedReason: "Indexing: Definition issues",
                        },
                        {
                            perfHintsCount: 1,
                            perfHintPrettifiedReason: "Storage: An extremely slow write to disk",
                        },
                    ],
                },
            ] satisfies RowItem[]);
        });

        it("can map multiple dbs on single node w/ alerts and performance hints", () => {
            const flatItems = [
                new databaseNotificationsItem("A", {
                    DatabaseName: "db1",
                    AlertsCount: 2,
                    Alerts: [
                        { Count: 2, Reason: "QueueSink_Error", PrettifiedReason: "Queue Sink: Invalid configuration" },
                    ],
                    PerformanceHintsCount: 2,
                    PerformanceHints: [
                        { Count: 1, Reason: "Indexing", PrettifiedReason: "Indexing: Definition issues" },
                        { Count: 1, Reason: "SlowIO", PrettifiedReason: "Storage: An extremely slow write to disk" },
                    ],
                }),
                new databaseNotificationsItem("A", {
                    DatabaseName: "db2",
                    AlertsCount: 1,
                    Alerts: [
                        { Count: 1, Reason: "QueueSink_Error", PrettifiedReason: "Queue Sink: Invalid configuration" },
                    ],
                    PerformanceHintsCount: 3,
                    PerformanceHints: [
                        { Count: 1, Reason: "Indexing", PrettifiedReason: "Indexing: Definition issues" },
                        { Count: 2, Reason: "SlowIO", PrettifiedReason: "Storage: An extremely slow write to disk" },
                    ],
                }),
            ];

            const result = mapToRowItems(flatItems);

            expect(result).toEqual([
                {
                    dbName: "db1",
                    nodeTag: "A",
                    alertsCount: 2,
                    perfHintsCount: 2,
                    subRows: [
                        {
                            alertsCount: 2,
                            alertPrettifiedReason: "Queue Sink: Invalid configuration",
                            perfHintsCount: 1,
                            perfHintPrettifiedReason: "Indexing: Definition issues",
                        },
                        {
                            perfHintsCount: 1,
                            perfHintPrettifiedReason: "Storage: An extremely slow write to disk",
                        },
                    ],
                },
                {
                    dbName: "db2",
                    nodeTag: "A",
                    alertsCount: 1,
                    perfHintsCount: 3,
                    subRows: [
                        {
                            alertsCount: 1,
                            alertPrettifiedReason: "Queue Sink: Invalid configuration",
                            perfHintsCount: 1,
                            perfHintPrettifiedReason: "Indexing: Definition issues",
                        },
                        {
                            perfHintsCount: 2,
                            perfHintPrettifiedReason: "Storage: An extremely slow write to disk",
                        },
                    ],
                },
            ] satisfies RowItem[]);
        });

        it("can map single db on multiple nodes w/ alerts and performance hints", () => {
            const flatItems = [
                new databaseNotificationsItem("A", {
                    DatabaseName: "db1",
                    AlertsCount: 2,
                    Alerts: [
                        { Count: 2, Reason: "QueueSink_Error", PrettifiedReason: "Queue Sink: Invalid configuration" },
                    ],
                    PerformanceHintsCount: 1,
                    PerformanceHints: [
                        { Count: 1, Reason: "Indexing", PrettifiedReason: "Indexing: Definition issues" },
                    ],
                }),
                new databaseNotificationsItem("B", {
                    DatabaseName: "db1",
                    AlertsCount: 1,
                    Alerts: [
                        { Count: 1, Reason: "QueueSink_Error", PrettifiedReason: "Queue Sink: Invalid configuration" },
                    ],
                    PerformanceHintsCount: 5,
                    PerformanceHints: [
                        { Count: 2, Reason: "Indexing", PrettifiedReason: "Indexing: Definition issues" },
                        { Count: 3, Reason: "SlowIO", PrettifiedReason: "Storage: An extremely slow write to disk" },
                    ],
                }),
            ];

            const result = mapToRowItems(flatItems);

            expect(result).toEqual([
                {
                    dbName: "db1",
                    alertsCount: 3,
                    perfHintsCount: 6,
                    subRows: [
                        {
                            nodeTag: "A",
                            alertsCount: 2,
                            perfHintsCount: 1,
                            subRows: [
                                {
                                    alertsCount: 2,
                                    alertPrettifiedReason: "Queue Sink: Invalid configuration",
                                    perfHintsCount: 1,
                                    perfHintPrettifiedReason: "Indexing: Definition issues",
                                },
                            ],
                        },
                        {
                            nodeTag: "B",
                            alertsCount: 1,
                            perfHintsCount: 5,
                            subRows: [
                                {
                                    alertsCount: 1,
                                    alertPrettifiedReason: "Queue Sink: Invalid configuration",
                                    perfHintsCount: 2,
                                    perfHintPrettifiedReason: "Indexing: Definition issues",
                                },
                                {
                                    perfHintsCount: 3,
                                    perfHintPrettifiedReason: "Storage: An extremely slow write to disk",
                                },
                            ],
                        },
                    ],
                },
            ] satisfies RowItem[]);
        });
    });
});
