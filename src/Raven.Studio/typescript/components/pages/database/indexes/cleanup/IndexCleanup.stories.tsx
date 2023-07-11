import { Meta, Story } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { IndexCleanup } from "./IndexCleanup";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Indexes/Index Cleanup",
    component: IndexCleanup,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof IndexCleanup>;

export const EmptyView: Story<typeof IndexCleanup> = () => {
    const { indexesService } = mockServices;
    indexesService.withGetStats();
    indexesService.withGetIndexMergeSuggestions();

    return <IndexCleanup db={DatabasesStubs.nonShardedClusterDatabase()} />;
};

// TODO kalczur
// export const SomeEmpty: ComponentStory<typeof IndexCleanup> = () => {
//     const mergableIndexes: IndexInfo[][] = [];
//     const subIndexes: IndexInfo[] = [
//         {
//             indexName: "Emp1",
//             containingIndexName: "Emp2",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//         {
//             indexName: "Emp3",
//             containingIndexName: "Emp4",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//     ];

//     const unusedIndexes: IndexInfo[] = [
//         {
//             indexName: "Emp1",
//             containingIndexName: "Emp2",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//         {
//             indexName: "Emp3",
//             containingIndexName: "Emp4",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//     ];
//     const unmergableIndexes: UnmergableIndexInfo[] = [];

//     return (
//         <IndexCleanup
//             mergableIndexes={mergableIndexes}
//             subIndexes={subIndexes}
//             unusedIndexes={unusedIndexes}
//             unmergableIndexes={unmergableIndexes}
//         />
//     );
// };

// export const CleanupSuggestions: ComponentStory<typeof IndexCleanup> = () => {
//     const mergableIndexes: IndexInfo[][] = [
//         [
//             {
//                 indexName: "Product/Search",
//                 lastQuery: new Date("2023-05-09"),
//                 lastIndexing: new Date("2023-05-08"),
//             },
//             {
//                 indexName: "Products/ByUnitOnStock",
//                 lastQuery: new Date("2023-05-07"),
//                 lastIndexing: new Date("2023-05-06"),
//             },
//         ],
//         [
//             {
//                 indexName: "Orders/Totals",
//                 lastQuery: new Date("2023-05-05"),
//                 lastIndexing: new Date("2023-05-04"),
//             },
//             {
//                 indexName: "OrdersFull",
//                 lastQuery: new Date("2023-02-03"),
//                 lastIndexing: new Date("2023-05-09"),
//             },
//             {
//                 indexName: "OrdersSub",
//                 lastQuery: new Date("2021-05-09"),
//                 lastIndexing: new Date("2022-05-09"),
//             },
//         ],
//     ];

//     const subIndexes: IndexInfo[] = [
//         {
//             indexName: "Emp1",
//             containingIndexName: "Emp2",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//         {
//             indexName: "Emp3",
//             containingIndexName: "Emp4",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//     ];

//     const unusedIndexes: IndexInfo[] = [
//         {
//             indexName: "Emp1",
//             containingIndexName: "Emp2",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//         {
//             indexName: "Emp3",
//             containingIndexName: "Emp4",
//             lastQuery: new Date("2023-05-09"),
//             lastIndexing: new Date("2023-05-08"),
//         },
//     ];

//     const unmergableIndexes: UnmergableIndexInfo[] = [
//         {
//             indexName: "Companies/StockPrices/TradeVolumeByMonth",
//             unmergableReason: "Cannot merge map/reduce indexes",
//         },
//         {
//             indexName: "Product/Rating",
//             unmergableReason: "Cannot merge map/reduce indexes",
//         },
//         {
//             indexName: "Orders/ByShipment/Location",
//             unmergableReason: "Cannot merge indexes that have a where clause",
//         },
//         {
//             indexName: "Orders/ByCompany",
//             unmergableReason: "Cannot merge map/reduce indexes",
//         },
//     ];

//     return (
//         <>
//             <IndexCleanup
//                 mergableIndexes={mergableIndexes}
//                 subIndexes={subIndexes}
//                 unusedIndexes={unusedIndexes}
//                 unmergableIndexes={unmergableIndexes}
//             />
//             <h1>*** Height check ***</h1>
//         </>
//     );
// };
