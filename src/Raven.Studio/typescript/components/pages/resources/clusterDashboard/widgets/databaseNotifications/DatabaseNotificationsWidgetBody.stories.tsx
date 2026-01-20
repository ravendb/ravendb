import { Meta } from "@storybook/react-webpack5";
import DatabaseNotificationsWidgetBody from "./DatabaseNotificationsWidgetBody";
import databaseNotificationsItem from "models/resources/widgets/databaseNotificationsItem";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ColumnFiltersState } from "@tanstack/react-table";
import { useState } from "storybook/internal/preview-api";
import Button from "react-bootstrap/Button";

export default {
    title: "Widgets/DatabaseNotificationsWidgetBody",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const Default = {
    render: () => {
        const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
        const [flatItems, setFlatItems] = useState(initialFlatItems);

        return (
            <div className="cluster-dashboard-container">
                <Button
                    onClick={() => setFlatItems((prev) => [...prev, databaseNotificationsItem.noData("D", "db1")])}
                    className="mb-2"
                >
                    Add empty item (for test purposes)
                </Button>
                <DatabaseNotificationsWidgetBody
                    flatItems={flatItems}
                    columnFilters={columnFilters}
                    setColumnFilters={setColumnFilters}
                />
            </div>
        );
    },
};

const initialFlatItems = [
    new databaseNotificationsItem("A", {
        DatabaseName: "db1",
        AlertsCount: 2,
        Alerts: [{ Count: 2, Reason: "BlockingTombstones", PrettifiedReason: "Blockage in tombstone deletion" }],
        PerformanceHintsCount: 1,
        PerformanceHints: [{ Count: 1, Reason: "Indexing", PrettifiedReason: "Indexing: Definition issues" }],
    }),
    new databaseNotificationsItem("B", {
        DatabaseName: "db1",
        AlertsCount: 1,
        Alerts: [{ Count: 1, Reason: "LowDiskSpace", PrettifiedReason: "Storage: Low free disk space" }],
        PerformanceHintsCount: 5,
        PerformanceHints: [
            { Count: 2, Reason: "UnusedCapacity", PrettifiedReason: "System: Not all cores are used" },
            { Count: 3, Reason: "SlowIO", PrettifiedReason: "Storage: An extremely slow write to disk" },
        ],
    }),
    databaseNotificationsItem.noData("C", "db1"),
    new databaseNotificationsItem("A", {
        DatabaseName: "db2",
        AlertsCount: 2,
        Alerts: [{ Count: 2, Reason: "QueueSink_Error", PrettifiedReason: "Queue Sink: Invalid configuration" }],
        PerformanceHintsCount: 1,
        PerformanceHints: [{ Count: 1, Reason: "Indexing", PrettifiedReason: "Indexing: Definition issues" }],
    }),
];
