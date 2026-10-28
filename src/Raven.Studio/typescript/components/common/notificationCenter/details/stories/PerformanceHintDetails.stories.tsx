import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { NotificationStubs } from "test/stubs/NotificationStubs";
import HugeDocumentsDetails from "../performanceHints/HugeDocumentsDetails";
import IndexingDetails from "../performanceHints/IndexingDetails";
import IndexingReferencesDetails from "../performanceHints/IndexingReferencesDetails";
import PagingDetails from "../performanceHints/PagingDetails";
import RequestLatencyDetails from "../performanceHints/RequestLatencyDetails";
import SlowIoDetails from "../performanceHints/SlowIoDetails";
import SlowSqlDetails from "../performanceHints/SlowSqlDetails";
import { storyActions, storyDate, times } from "./notificationDetailsStoryUtils";

export default {
    title: "Shell/Notification Details/Performance Hints",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const HugeDocuments: StoryObj = {
    render: () => {
        const documents = times(25, (index) => ({
            Id: `orders/${index + 1}-A`,
            Size: (index + 1) * 3_500_000,
            Date: storyDate(index * 7),
        }));

        const hint = NotificationStubs.performanceHint(
            "HugeDocuments",
            { HugeDocuments: Object.fromEntries(documents.map((x) => [x.Id, x])) },
            {
                Title: "Huge documents",
                Message: "We have detected usage of huge documents. Consider splitting them into smaller documents.",
            }
        );

        return <HugeDocumentsDetails notification={hint} actions={storyActions} />;
    },
};

export const Indexing: StoryObj = {
    render: () => {
        const hint = NotificationStubs.performanceHint(
            "Indexing",
            {
                Warnings: {
                    "Orders/ByProduct": times(4, (index) => ({
                        NumberOfExceedingDocuments: 1200 * (index + 1),
                        MaxNumberOutputsPerDocument: 150 + index * 25,
                        SampleDocumentId: `orders/${index + 100}-A`,
                        Suggestion: "Consider using a fanout index or reducing the number of index entries.",
                        LastWarningTime: storyDate(index * 30),
                    })),
                    "Companies/ByEmployee": [
                        {
                            NumberOfExceedingDocuments: 320,
                            MaxNumberOutputsPerDocument: 2048,
                            SampleDocumentId: "companies/77-A",
                            Suggestion: "Make sure that the index does not output an entry for every array item.",
                            LastWarningTime: storyDate(5),
                        },
                    ],
                },
            },
            {
                Title: "Number of map results produced by an index exceeds the limit",
                Message: "Some indexes produce a lot of map results per document.",
            }
        );

        return <IndexingDetails notification={hint} actions={storyActions} />;
    },
};

export const IndexingReferences: StoryObj = {
    render: () => {
        const hint = NotificationStubs.performanceHint(
            "Indexing_References",
            {
                Warnings: {
                    "Orders/ByCompany": {
                        LastWarningTime: storyDate(3),
                        Top10LoadedReferences: Object.fromEntries(
                            times(10, (index) => [
                                `orders/${index + 1}-A`,
                                { ReferenceId: `companies/${index + 1}-A`, NumberOfLoads: 10_000 - index * 750 },
                            ])
                        ),
                    },
                    "Products/ByCategory": {
                        LastWarningTime: storyDate(45),
                        Top10LoadedReferences: {
                            "products/1-A": { ReferenceId: "categories/1-A", NumberOfLoads: 4_200 },
                            "products/2-A": { ReferenceId: "categories/2-A", NumberOfLoads: 1_100 },
                        },
                    },
                },
            },
            {
                Title: "High number of references loaded by an index",
                Message: "Indexes load a lot of referenced documents. Click a row to see the top references.",
            }
        );

        return <IndexingReferencesDetails notification={hint} actions={storyActions} />;
    },
};

export const Paging: StoryObj = {
    render: () => {
        const hint = NotificationStubs.performanceHint(
            "Paging",
            {
                Actions: {
                    "Query (Orders/ByCompany)": times(12, (index) => ({
                        Details: `from index 'Orders/ByCompany' where Company = 'companies/${index + 1}-A'`,
                        Duration: 120 + index * 15,
                        NumberOfResults: 2_048 + index * 100,
                        Occurrence: storyDate(index * 11),
                        PageSize: index === 0 ? 2147483647 : 1024,
                        TotalDocumentsSizeInBytes: 12_500_000 + index * 1_000_000,
                    })),
                    GetDocumentsStartingWith: [
                        {
                            Details: "startsWith: employees/",
                            Duration: 85,
                            NumberOfResults: 1_500,
                            Occurrence: storyDate(2),
                            PageSize: 1_500,
                            TotalDocumentsSizeInBytes: 4_300_000,
                        },
                    ],
                },
            },
            {
                Title: "Page size too big",
                Message: "We have detected that some of the requests are returning a lot of results.",
            }
        );

        return <PagingDetails notification={hint} actions={storyActions} />;
    },
};

export const RequestLatency: StoryObj = {
    render: () => {
        const hint = NotificationStubs.performanceHint(
            "RequestLatency",
            {
                RequestLatencies: {
                    Query: times(15, (index) => ({
                        Action: "Query",
                        Date: storyDate(index * 4),
                        Duration: 1_500 + index * 900,
                        Query: `from Orders where ShipTo.City = 'City ${index}' order by OrderedAt desc`,
                    })),
                    BulkInsert: [{ Action: "BulkInsert", Date: storyDate(1), Duration: 65_000, Query: null }],
                },
            },
            {
                Title: "Request latency is too high",
                Message: "We have detected that some requests take a long time to complete.",
            }
        );

        return <RequestLatencyDetails notification={hint} actions={storyActions} />;
    },
};

export const SlowIo: StoryObj = {
    render: () => {
        const writes = times(8, (index) => ({
            Key: `write-${index}`,
            Path: `C:\\RavenDB\\Databases\\db1\\Journals\\${String(index + 1).padStart(19, "0")}.journal`,
            Type: index % 2 === 0 ? ("JournalWrite" as const) : ("DataFlush" as const),
            Date: storyDate(index * 9),
            DataWrittenInMb: 1.5 + index,
            DurationInSec: 3.2 + index * 0.6,
            SpeedInMbPerSec: (1.5 + index) / (3.2 + index * 0.6),
        }));

        const hint = NotificationStubs.performanceHint(
            "SlowIO",
            { Writes: Object.fromEntries(writes.map((x) => [x.Key, x])) },
            {
                Title: "An extremely slow write to disk",
                Message: "We have detected very slow writes to the disk.",
            }
        );

        return <SlowIoDetails notification={hint} actions={storyActions} />;
    },
};

export const SlowSql: StoryObj = {
    render: () => {
        const hint = NotificationStubs.performanceHint(
            "SqlEtl_SlowSql",
            {
                Statements: times(10, (index) => ({
                    Date: storyDate(index * 6),
                    Duration: 3_000 + index * 450,
                    Statement: `INSERT INTO Orders (Id, Company, Total)\nVALUES ('orders/${index + 1}-A', 'companies/${index + 1}-A', ${index * 125.5})`,
                })),
            },
            {
                Title: "Slow SQL detected",
                Message:
                    "SQL ETL task 'Orders to SQL' runs slow SQL statements. Click a row to see the full statement.",
            }
        );

        return <SlowSqlDetails notification={hint} actions={storyActions} />;
    },
};
