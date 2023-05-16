import { ComponentMeta, ComponentStory } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { IndexInfo, IndexCleanup, UnmergableIndexInfo } from "./IndexCleanup";

export default {
    title: "Pages/Index Cleanup",
    component: IndexCleanup,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof IndexCleanup>;

export const EmptyView: ComponentStory<typeof IndexCleanup> = () => {
    const mergableIndexes: IndexInfo[][] = [];
    const subIndexes: IndexInfo[] = [];
    const unusedIndexes: IndexInfo[] = [];
    const unmergableIndexes: UnmergableIndexInfo[] = [];

    return (
        <IndexCleanup
            mergableIndexes={mergableIndexes}
            subIndexes={subIndexes}
            unusedIndexes={unusedIndexes}
            unmergableIndexes={unmergableIndexes}
        />
    );
};

export const SomeEmpty: ComponentStory<typeof IndexCleanup> = () => {
    const mergableIndexes: IndexInfo[][] = [];
    const subIndexes: IndexInfo[] = [
        {
            indexName: "Emp1",
            containingIndexName: "Emp2",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
        {
            indexName: "Emp3",
            containingIndexName: "Emp4",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
    ];

    const unusedIndexes: IndexInfo[] = [
        {
            indexName: "Emp1",
            containingIndexName: "Emp2",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
        {
            indexName: "Emp3",
            containingIndexName: "Emp4",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
    ];
    const unmergableIndexes: UnmergableIndexInfo[] = [];

    return (
        <IndexCleanup
            mergableIndexes={mergableIndexes}
            subIndexes={subIndexes}
            unusedIndexes={unusedIndexes}
            unmergableIndexes={unmergableIndexes}
        />
    );
};

export const CleanupSugestions: ComponentStory<typeof IndexCleanup> = () => {
    const mergableIndexes: IndexInfo[][] = [
        [
            {
                indexName: "Product/Search",
                lastQuery: new Date("2023-05-09"),
                lastIndexing: new Date("2023-05-08"),
            },
            {
                indexName: "Products/ByUnitOnStock",
                lastQuery: new Date("2023-05-07"),
                lastIndexing: new Date("2023-05-06"),
            },
        ],
        [
            {
                indexName: "Orders/Totals",
                lastQuery: new Date("2023-05-05"),
                lastIndexing: new Date("2023-05-04"),
            },
            {
                indexName: "OrdersFull",
                lastQuery: new Date("2023-02-03"),
                lastIndexing: new Date("2023-05-09"),
            },
            {
                indexName: "OrdersSub",
                lastQuery: new Date("2021-05-09"),
                lastIndexing: new Date("2022-05-09"),
            },
        ],
    ];

    const subIndexes: IndexInfo[] = [
        {
            indexName: "Emp1",
            containingIndexName: "Emp2",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
        {
            indexName: "Emp3",
            containingIndexName: "Emp4",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
    ];

    const unusedIndexes: IndexInfo[] = [
        {
            indexName: "Emp1",
            containingIndexName: "Emp2",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
        {
            indexName: "Emp3",
            containingIndexName: "Emp4",
            lastQuery: new Date("2023-05-09"),
            lastIndexing: new Date("2023-05-08"),
        },
    ];

    const unmergableIndexes: UnmergableIndexInfo[] = [
        {
            indexName: "Companies/StockPrices/TradeVolumeByMonth",
            unmergableReason: "Cannot merge map/reduce indexes",
        },
        {
            indexName: "Product/Rating",
            unmergableReason: "Cannot merge map/reduce indexes",
        },
        {
            indexName: "Orders/ByShipment/Location",
            unmergableReason: "Cannot merge indexes that have a where clause",
        },
        {
            indexName: "Orders/ByCompany",
            unmergableReason: "Cannot merge map/reduce indexes",
        },
    ];

    return (
        <>
            <IndexCleanup
                mergableIndexes={mergableIndexes}
                subIndexes={subIndexes}
                unusedIndexes={unusedIndexes}
                unmergableIndexes={unmergableIndexes}
            />
            <h1>*** Height check ***</h1>
        </>
    );
};
