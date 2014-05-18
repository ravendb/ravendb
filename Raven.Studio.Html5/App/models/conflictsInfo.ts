import conflict = require("models/conflict");
import abstractQueryResult = require("models/abstractQueryResult");

class conflictsInfo extends abstractQueryResult {

    results: Array<conflict>;

    constructor(dto: conflictsInfoDto) {
        super(dto);
        this.results = dto.Results.map(d => new conflict(d));
    }
}

export = conflictsInfo;