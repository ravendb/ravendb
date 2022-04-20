import assertUnreachable from "./assertUnreachable";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { IndexNodeInfoDetails, IndexSharedInfo, IndexStatus } from "../models/indexes";
import IndexType = Raven.Client.Documents.Indexes.IndexType;
import collection from "models/database/documents/collection";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;

export default class IndexUtils {
    static readonly DefaultIndexGroupName = "Other";
    static readonly AutoIndexPrefix = "Auto/";
    static readonly SideBySideIndexPrefix = "ReplacementOf/";

    static isAutoIndex(index: IndexSharedInfo) {
        switch (index.type) {
            case "Map":
            case "MapReduce":
                return false;
            case "AutoMap":
            case "AutoMapReduce":
                return true;
            default:
                return this.name.startsWith(IndexUtils.AutoIndexPrefix);
        }
    }

    static formatLockMode(lockMode: IndexLockMode) {
        switch (lockMode) {
            case "LockedIgnore":
                return "Locked (ignore)";
            case "LockedError":
                return "Locked (error)";
            case "Unlock":
                return "Unlock";
            default:
                assertUnreachable(lockMode);
        }
    }

    static formatStatus(status: IndexStatus) {
        switch (status) {
            case "RollingDeployment":
                return "Rolling deployment";
            case "ErrorOrFaulty":
                return "Error, Faulty";
            default:
                return status;
        }
    }

    static formatType(indexType: IndexType) {
        switch (indexType) {
            case "Map":
                return "Map";
            case "MapReduce":
                return "Map-Reduce";
            case "AutoMap":
                return "Auto Map";
            case "AutoMapReduce":
                return "Auto Map-Reduce";
            default:
                return indexType;
        }
    }

    static isFaulty(index: IndexSharedInfo) {
        return index.type === "Faulty";
    }

    static isErrorState(index: IndexNodeInfoDetails) {
        return index.state === "Error";
    }

    static isPausedState(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        const localStatusIsPaused = index.status === "Paused";
        const globalStatusIsPaused = globalIndexingStatus === "Paused";
        const isInDisableState = IndexUtils.isDisabledState(index, globalIndexingStatus);
        return (localStatusIsPaused || globalStatusIsPaused) && !isInDisableState;
    }

    static isDisabledState(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        const stateIsDisabled = index.state === "Disabled";
        const globalStatusIsDisabled = globalIndexingStatus === "Disabled";
        return stateIsDisabled || globalStatusIsDisabled;
    }

    static isIdleState(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        const stateIsIdle = index.state === "Idle";
        const globalStatusIsNotDisabled = globalIndexingStatus === "Running";
        const isPaused = IndexUtils.isPausedState(index, globalIndexingStatus);
        return stateIsIdle && globalStatusIsNotDisabled && !isPaused;
    }

    static isNormalState(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        const stateIsNormal = index.state === "Normal";
        const localStatusIsNormalOrPending = index.status === "Running" || index.status === "Pending";
        const globalStatusIsNotDisabled = globalIndexingStatus === "Running";
        return stateIsNormal && globalStatusIsNotDisabled && localStatusIsNormalOrPending;
    }

    static getIndexGroupName(index: IndexSharedInfo, allCollections: collection[]) {
        const collections = index.collections.map((c) => {
            // If collection already exists - use its exact name
            const x = allCollections.find((x) => x.name.toLowerCase() === c.toLowerCase());
            if (x) {
                return x.name;
            }
            // If collection does not exist - capitalize to be standard looking
            else {
                return _.capitalize(c);
            }
        });

        if (collections && collections.length) {
            return collections
                .slice(0)
                .sort((l, r) => (l.toLowerCase() > r.toLowerCase() ? 1 : -1))
                .join(", ");
        } else {
            return IndexUtils.DefaultIndexGroupName;
        }
    }

    static isSideBySide(index: IndexSharedInfo) {
        return index.name.startsWith(IndexUtils.SideBySideIndexPrefix);
    }

    static canBePaused(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        const localStatusIsNotDisabled = index.status !== "Disabled";
        const notInPausedState = !IndexUtils.isPausedState(index, globalIndexingStatus);
        return localStatusIsNotDisabled && notInPausedState;
    }

    static canBeResumed(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        const localStatusIsNotDisabled = index.status !== "Disabled";
        const inPausedState = IndexUtils.isPausedState(index, globalIndexingStatus);
        const errored = IndexUtils.isErrorState(index);
        return localStatusIsNotDisabled && inPausedState && !errored;
    }

    static canBeDisabled(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        return !IndexUtils.isDisabledState(index, globalIndexingStatus);
    }

    static canBeEnabled(index: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
        const disabled = IndexUtils.isDisabledState(index, globalIndexingStatus);
        const errored = IndexUtils.isErrorState(index);
        return disabled || errored;
    }

    static isPending(index: IndexNodeInfoDetails) {
        return index.status === "Pending";
    }
}
