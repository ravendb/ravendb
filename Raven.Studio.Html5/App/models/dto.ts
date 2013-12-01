interface collectionInfoDto {
	Results: documentDto[];
	Includes: any[];
	IsStale: boolean;
	IndexTimestamp: string;
	TotalResults: number;
	SkippedResults: number;
	IndexName: string; 
	IndexEtag: string;
	ResultEtag: string;
	Highlightings: any;
	NonAuthoritativeInformation: boolean;
	LastQueryTime: string;
	DurationMilliseconds: number;
}

interface documentDto {
	'@metadata': documentMetadataDto;
}

interface documentMetadataDto {
	'Raven-Entity-Name': string;
	'Raven-Clr-Type': string;
	'Non-Authoritative-Information': boolean;
	'@id': string;
	'Temp-Index-Score': number;
	'Last-Modified': string;
	'Raven-Last-Modified': string;
	'@etag': string;
}

interface documentStatistics {
    ApproximateTaskCount: number;
    CountOfDocuments: number;
    CountOfIndexes: number;
    CurrentNumberOfItemsToIndexInSingleBatch: number;
    CurrentNumberOfItemsToReduceInSingleBatch: number;
    DatabaseId: string;
    DatabaseTransactionVersionSizeInMB: number;
    Errors: Array<any>;
    Extensions: Array<any>;
    InMemoryIndexingQueueSize: number;
    Indexes: Array<any>;
    LastAttachmentEtag: string;
    LastDocEtag: string;
    Prefetches: Array<any>;
    StaleIndexes: Array<any>;
    Triggers: Array<any>;
}