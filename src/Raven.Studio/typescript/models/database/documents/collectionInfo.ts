import document = require("models/database/documents/document");

class collectionInfo {
    results: Array<document>;
    totalResults: number;

    //TODO: use document preview endpoint
    constructor(dto: collectionInfoDto) {
        this.results = dto.Results.map(d => new document(d));
        this.totalResults = dto.TotalResults;
    }
}

export = collectionInfo;
