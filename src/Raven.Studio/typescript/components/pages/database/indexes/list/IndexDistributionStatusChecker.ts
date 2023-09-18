import { IndexSharedInfo } from "components/models/indexes";

export default class IndexDistributionStatusChecker {
    private readonly index: IndexSharedInfo;

    constructor(index: IndexSharedInfo) {
        this.index = index;
    }

    everyFailure = () => {
        const allShards = this.index.nodesInfo.map((x) => x.location.shardNumber);

        let isAnyShardFailure = false;

        for (const shard of allShards) {
            const isEveryNodeForShardFailure = this.index.nodesInfo
                .filter((x) => x.location.shardNumber === shard)
                .every((x) => x.status === "failure");

            if (isEveryNodeForShardFailure) {
                isAnyShardFailure = true;
                break;
            }
        }

        return isAnyShardFailure;
    };

    someFailure = (): boolean => this.index.nodesInfo.some((x) => x.status === "failure");
    everyFaulty = (): boolean => this.index.nodesInfo.every((x) => x.details?.faulty);
    everyErrors = (): boolean => this.index.nodesInfo.every((x) => x.details?.state === "Error");
    everyDisabled = (): boolean => this.index.nodesInfo.every((x) => x.details?.status === "Disabled");
    everyPaused = (): boolean => this.index.nodesInfo.every((x) => x.details?.status === "Paused");
    everyPending = (): boolean => this.index.nodesInfo.every((x) => x.details?.status === "Pending");
    someErrors = (): boolean => this.index.nodesInfo.some((x) => x.details?.state === "Error");
    someFaulty = (): boolean => this.index.nodesInfo.some((x) => x.details?.faulty);
    someDisabled = (): boolean => this.index.nodesInfo.some((x) => x.details?.status === "Disabled");
    somePaused = (): boolean => this.index.nodesInfo.some((x) => x.details?.status === "Paused");
    somePending = (): boolean => this.index.nodesInfo.some((x) => x.details?.status === "Pending");
    someStale = (): boolean => this.index.nodesInfo.some((x) => x.details?.stale);
}
