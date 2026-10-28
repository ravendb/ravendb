import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { NotificationStubs } from "test/stubs/NotificationStubs";
import virtualBulkInsert from "common/notifications/models/virtualBulkInsert";
import virtualBulkInsertFailures from "common/notifications/models/virtualBulkInsertFailures";
import virtualDeleteByQuery from "common/notifications/models/virtualDeleteByQuery";
import virtualDeleteByQueryFailures from "common/notifications/models/virtualDeleteByQueryFailures";
import virtualUpdateByQuery from "common/notifications/models/virtualUpdateByQuery";
import virtualUpdateByQueryFailures from "common/notifications/models/virtualUpdateByQueryFailures";
import {
    VirtualBulkInsertDetails,
    VirtualBulkInsertFailuresDetails,
    VirtualDeleteByQueryDetails,
    VirtualDeleteByQueryFailuresDetails,
    VirtualUpdateByQueryDetails,
    VirtualUpdateByQueryFailuresDetails,
} from "../virtualOperations/VirtualOperationDetails";
import { storyActions, storyDate, times } from "./notificationDetailsStoryUtils";

export default {
    title: "Shell/Notification Details/Virtual Operations",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

function bulkInsertItem(index: number): virtualBulkOperationItem {
    return {
        id: `bulk-insert-${index}`,
        operationId: index,
        date: storyDate(index * 5),
        duration: 1_200 + index * 300,
        totalItemsProcessed: 10_000 + index * 500,
        documentsProcessed: 9_000 + index * 400,
        attachmentsProcessed: 500,
        countersProcessed: 300 + index * 50,
        timeSeriesProcessed: 200 + index * 50,
    };
}

function failureItem(index: number): virtualBulkOperationFailureItem {
    return {
        id: `failure-${index}`,
        operationId: index,
        date: storyDate(index * 5),
        duration: 400 + index * 100,
        errorMsg: "Document has a concurrency conflict",
        error:
            "Raven.Client.Exceptions.ConcurrencyException: Document orders/1-A has change vector A:10, " +
            "but the expected change vector was A:9.",
    };
}

function queryItem(index: number): queryBasedVirtualBulkOperationItem {
    return {
        ...bulkInsertItem(index),
        id: `query-${index}`,
        query: `from Orders where Freight > ${index * 10}`,
        indexOrCollectionUsed: index % 2 === 0 ? "Orders" : "Orders/ByCompany",
    };
}

function queryFailureItem(index: number): queryBasedVirtualBulkOperationFailureItem {
    return {
        ...bulkInsertItem(index),
        id: `query-failure-${index}`,
        query: `from Orders where Freight > ${index * 10}`,
        errorMsg: "The index is stale",
        error: "Raven.Client.Exceptions.Documents.Indexes.IndexStaleException: Index 'Orders/ByCompany' is stale.",
    };
}

export const BulkInsert: StoryObj = {
    render: () => {
        const notification = NotificationStubs.virtualOperation(virtualBulkInsert, times(12, bulkInsertItem));
        return <VirtualBulkInsertDetails notification={notification} actions={storyActions} />;
    },
};

export const BulkInsertFailures: StoryObj = {
    render: () => {
        const notification = NotificationStubs.virtualOperation(virtualBulkInsertFailures, times(4, failureItem));
        return <VirtualBulkInsertFailuresDetails notification={notification} actions={storyActions} />;
    },
};

export const UpdateByQuery: StoryObj = {
    render: () => {
        const notification = NotificationStubs.virtualOperation(virtualUpdateByQuery, times(8, queryItem));
        return <VirtualUpdateByQueryDetails notification={notification} actions={storyActions} />;
    },
};

export const UpdateByQueryFailures: StoryObj = {
    render: () => {
        const notification = NotificationStubs.virtualOperation(
            virtualUpdateByQueryFailures,
            times(3, queryFailureItem)
        );
        return <VirtualUpdateByQueryFailuresDetails notification={notification} actions={storyActions} />;
    },
};

export const DeleteByQuery: StoryObj = {
    render: () => {
        const notification = NotificationStubs.virtualOperation(virtualDeleteByQuery, times(8, queryItem));
        return <VirtualDeleteByQueryDetails notification={notification} actions={storyActions} />;
    },
};

export const DeleteByQueryFailures: StoryObj = {
    render: () => {
        const notification = NotificationStubs.virtualOperation(
            virtualDeleteByQueryFailures,
            times(3, queryFailureItem)
        );
        return <VirtualDeleteByQueryFailuresDetails notification={notification} actions={storyActions} />;
    },
};
