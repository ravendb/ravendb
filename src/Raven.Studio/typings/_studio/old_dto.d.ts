/// <reference path="../../typescript/common/constants.ts"/>

interface collectionInfoDto extends Raven.Client.Documents.Queries.QueryResult<Array<documentDto>, any> {
}

interface serverBuildVersionDto {
    BuildVersion: number;
    ProductVersion: string;
    CommitHash: string;
    FullVersion: string;
}

interface clientBuildVersionDto {
    Version: string;
}



interface documentBase extends dictionary<any> {
    getId(): string;
    getUrl(): string;
    getDocumentPropertyNames(): Array<string>;
}


interface resourceStyleMap {
    resourceName: string;
    styleMap: any;
}

interface changesApiEventDto {
    Time: string; // ISO date string
    Type: string;
    Value?: any;
}

interface mergeResult {
  Document: string;
  Metadata: string;
}

