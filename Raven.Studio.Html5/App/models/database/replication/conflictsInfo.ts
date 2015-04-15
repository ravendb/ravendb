import conflict = require("models/database/replication/conflict");
import abstractQueryResult = require("models/database/query/abstractQueryResult");

class conflictsInfo extends abstractQueryResult {

    results: Array<conflict>;

    constructor(dto: conflictsInfoDto) {
        super(dto);
        this.results = dto.Results.map(d => new conflict(d));
    }
}

export = conflictsInfo;