import assertUnreachable from "./assertUnreachable";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { IndexNodeInfoDetails, IndexSharedInfo, IndexStatus } from "../models/indexes";
import IndexType = Raven.Client.Documents.Indexes.IndexType;
import collection from "models/database/documents/collection";

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

    static isPausedState(index: IndexNodeInfoDetails) {
        /* TODO
        const localStatusIsPaused = this.status() === "Paused";
            const globalStatusIsPaused = this.globalIndexingStatus() === "Paused";
            const isInDisableState = this.isDisabledState();
            return (localStatusIsPaused || globalStatusIsPaused) && !isInDisableState;
         */
        
        return index.status === "Paused";
    }
    
    static isDisabledState(index: IndexNodeInfoDetails) {
        const stateIsDisabled = index.state === "Disabled";
        //TODO:const globalStatusIsDisabled = this.globalIndexingStatus() === "Disabled";
        return stateIsDisabled; //TODO: || globalStatusIsDisableds
    }
    
    static isIdleState(index: IndexNodeInfoDetails) {
        /* TODO
        const stateIsIdle = this.state() === "Idle";
            const globalStatusIsNotDisabled = this.globalIndexingStatus() === "Running";
            const isPaused = this.isPausedState();
            return stateIsIdle && globalStatusIsNotDisabled && !isPaused;
         */
        return index.state === "Idle";
    }

    static isNormalState(index: IndexNodeInfoDetails) {
        /* TODO
          const stateIsNormal = this.state() === "Normal";
            const localStatusIsNormalOrPending = this.status() === "Running" || this.status() === "Pending";
            const globalStatusIsNotDisabled = this.globalIndexingStatus() === "Running";
            return stateIsNormal && globalStatusIsNotDisabled && localStatusIsNormalOrPending;
         */
        
        return index.state === "Normal";
    }

    static getIndexGroupName(index: IndexSharedInfo, allCollections: collection[]) {
        const collections = index.collections.map(c => {
            // If collection already exists - use its exact name
            const x = allCollections.find(x => x.name.toLowerCase() === c.toLowerCase());
            if (x) {
                return x.name;
            }
            // If collection does not exist - capitalize to be standard looking 
            else {
                return _.capitalize(c);
            }
        });

        if (collections && collections.length) {
            return collections.slice(0).sort((l, r) => l.toLowerCase() > r.toLowerCase() ? 1 : -1).join(", ");
        } else {
            return IndexUtils.DefaultIndexGroupName;
        }
    }
    
    static isSideBySide(index: IndexSharedInfo) {
        return index.name.startsWith(IndexUtils.SideBySideIndexPrefix);
    }
    
}
