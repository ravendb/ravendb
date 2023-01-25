import { loadStatus, locationAwareLoadableData } from "components/models/common";

export interface DetailedIndexStats {
    perLocationStatus: locationAwareLoadableData<never>[];
    groups: IndexGroupStats[];
    globalState: loadStatus;
    noData: boolean;
}

export interface IndexGroupStats {
    type: IndexType;
    indexes: PerIndexStats[];
}

export interface PerIndexStats {
    name: string;
    type: IndexType;
    isReduceIndex: boolean;
    details: PerLocationIndexStats[];
}

export interface PerLocationIndexStats {
    entriesCount: number;
    errorsCount: number;

    isFaultyIndex: boolean;
    isStale: boolean;

    mapAttempts: number;
    mapSuccesses: number;
    mapErrors: number;

    mapReferenceSuccesses: number;
    mapReferenceErrors: number;
    mapReferenceAttempts: number;

    mappedPerSecondRate: number;
    reducedPerSecondRate: number;

    reduceAttempts: number;
    reduceSuccesses: number;
    reduceErrors: number;
}
