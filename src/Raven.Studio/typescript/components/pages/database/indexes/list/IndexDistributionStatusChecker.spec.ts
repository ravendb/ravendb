import { IndexSharedInfo } from "components/models/indexes";
import IndexDistributionStatusChecker from "./IndexDistributionStatusChecker";
import { loadStatus } from "components/models/common";

describe("IndexDistributionStatusChecker", () => {
    describe("everyFailure", () => {
        function getNodesInfo(nodeTag: string, shardNumber: number, status: loadStatus) {
            return {
                location: { nodeTag, shardNumber },
                status,
            };
        }

        it("false when single node is not failure", () => {
            const statusChecker = new IndexDistributionStatusChecker({
                nodesInfo: [getNodesInfo("A", undefined, "success")],
            } as IndexSharedInfo);

            expect(statusChecker.everyFailure()).toBeFalse();
        });

        it("true when single node is failure", () => {
            const statusChecker = new IndexDistributionStatusChecker({
                nodesInfo: [getNodesInfo("A", undefined, "failure")],
            } as IndexSharedInfo);

            expect(statusChecker.everyFailure()).toBeTrue();
        });

        it("false when some nodes are failure", () => {
            const statusChecker = new IndexDistributionStatusChecker({
                nodesInfo: [
                    getNodesInfo("A", undefined, "failure"),
                    getNodesInfo("B", undefined, "failure"),
                    getNodesInfo("C", undefined, "success"),
                ],
            } as IndexSharedInfo);

            expect(statusChecker.everyFailure()).toBeFalse();
        });

        it("true when all nodes are failure", () => {
            const statusChecker = new IndexDistributionStatusChecker({
                nodesInfo: [
                    getNodesInfo("A", undefined, "failure"),
                    getNodesInfo("B", undefined, "failure"),
                    getNodesInfo("C", undefined, "failure"),
                ],
            } as IndexSharedInfo);

            expect(statusChecker.everyFailure()).toBeTrue();
        });

        it("true when all nodes for shard are failure", () => {
            const statusChecker = new IndexDistributionStatusChecker({
                nodesInfo: [
                    getNodesInfo("A", 1, "failure"),
                    getNodesInfo("B", 2, "success"),
                    getNodesInfo("C", 1, "failure"),
                ],
            } as IndexSharedInfo);

            expect(statusChecker.everyFailure()).toBeTrue();
        });

        it("false when some nodes for shard are failure", () => {
            const statusChecker = new IndexDistributionStatusChecker({
                nodesInfo: [
                    getNodesInfo("A", 1, "failure"),
                    getNodesInfo("B", 2, "success"),
                    getNodesInfo("C", 1, "success"),
                ],
            } as IndexSharedInfo);

            expect(statusChecker.everyFailure()).toBeFalse();
        });
    });
});
