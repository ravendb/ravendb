import document = require("models/database/documents/document");
import abstractQueryResult = require("models/database/query/abstractQueryResult");

class collectionInfo extends abstractQueryResult {
	results: Array<document>;

    constructor(dto: collectionInfoDto) {
        super(dto);
		this.results = dto.Results.map(d => new document(d))
	}
}

export = collectionInfo;