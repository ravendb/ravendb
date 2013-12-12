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

interface apiKeyDto {
    Name: string;
    Secret: string;
    Enabled: boolean;
    Databases: Array<apiKeyDatabaseDto>;
}

interface apiKeyDatabaseDto {
    TenantId: string;
    Admin: boolean;
    ReadOnly: boolean;
}

interface buildVersionDto {
    ProductVersion: string;
    BuildVersion: string;
}

interface licenseStatusDto {
    Message: string;
    Status: string;
    Error: boolean;
    IsCommercial: boolean;
    ValidCommercialLicenseSeen: boolean;
    Attributes: {
        periodicBackup: string;
        encryption: string;
        compression: string;
        quotas: string;
        authorization: string;
        documentExpiration: string;
        replication: string;
        versioning: string;
        version: string;
        allowWindowsClustering: string;
        OEM: string;
        numberOfDatabases: string;
        maxSizeInMb: string;
        maxRamUtilization: string;
        maxParallelism: string;
    }
}