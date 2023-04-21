export interface IndexItem {
    sharedInfo: {
        name: string;
        isReduceIndex: boolean;
        type: Raven.Client.Documents.Indexes.IndexType;
    };

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
