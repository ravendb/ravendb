import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { BulkIndexOperationConfirm } from "./BulkIndexOperationConfirm";
import { IndexSharedInfo } from "components/models/indexes";
import { DatabaseActionContexts } from "components/common/MultipleDatabaseLocationSelector";

describe("BulkIndexOperationConfirm", () => {
    const allActionContexts: DatabaseActionContexts[] = [{ nodeTag: "A" }];

    function createIndex(name: string, status: "Running" | "Disabled" | "Paused") {
        return {
            name,
            nodesInfo: [{ details: { status } }],
        } as IndexSharedInfo;
    }

    it("opens the only index group by default", async () => {
        const { screen } = rtlRender(
            <BulkIndexOperationConfirm
                type="disable"
                indexes={[createIndex("Users/ByName", "Running")]}
                toggle={jest.fn()}
                allActionContexts={allActionContexts}
                onConfirm={jest.fn()}
            />
        );

        expect(await screen.findByText("Users/ByName")).toBeInTheDocument();
    });

    it("keeps groups collapsed by default when there are multiple groups", () => {
        const { screen } = rtlRender(
            <BulkIndexOperationConfirm
                type="disable"
                indexes={[createIndex("Users/ByName", "Running"), createIndex("Orders/ByDate", "Disabled")]}
                toggle={jest.fn()}
                allActionContexts={allActionContexts}
                onConfirm={jest.fn()}
            />
        );

        expect(screen.queryByText("Users/ByName")).not.toBeInTheDocument();
        expect(screen.queryByText("Orders/ByDate")).not.toBeInTheDocument();
    });
});
