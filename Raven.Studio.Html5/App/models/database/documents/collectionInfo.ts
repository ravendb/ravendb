import document = require("models/database/documents/document");

class collectionInfo {
    results: Array<document>;
    totalResults: number;

    constructor(dto: documentPreviewDto) {
        this.results = dto.Results.map(d => new document(d));
        this.totalResults = dto.TotalResults;
    }
}

export = collectionInfo;
