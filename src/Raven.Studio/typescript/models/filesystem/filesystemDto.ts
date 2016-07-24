const enum synchronizationType {
    Unknown = 0,
    ContentUpdate = 1,
    MetadataUpdate = 2,
    Rename = 3,
    Delete = 4,
}

interface synchronizationDetailsDto {
    FileName: string;
    FileETag: string;
    DestinationUrl: string;
    Type: string;
}

interface synchronizationReportDto {
    FileName: string;
    FileETag: string;
    BytesTransfered: number;
    BytesCopied: number,
    NeedListLength: number;
    Exception: string,
    Type: string;
}

const enum synchronizationActivity {
    Unknown,
    Active,
    Pending,
    Finished
}

interface configurationSearchResultsDto {
    ConfigNames: string[];
    TotalCount: number;
    Start: number;
    PageSize: number;
}

interface sourceSynchronizationInformationDto {
   LastSourceFileEtag: string;
   SourceServerUrl: string;
   DestinationServerId: string;
}
