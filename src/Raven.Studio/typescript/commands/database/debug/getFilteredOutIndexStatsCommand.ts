import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import d3 = require("d3");

class getFilteredOutIndexStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<filteredOutIndexStatDto[]> {
        var url = "/debug/filtered-out-indexes";//TODO: use endpoints
        var parser = d3.time.format.iso;

        return this.query<filteredOutIndexStatDto[]>(url, null, this.db, result => {
            result.map((item: filteredOutIndexStatDto) => {
                item.TimestampParsed = parser.parse(item.Timestamp);
            });
            return result;
        });
    }

}

export = getFilteredOutIndexStatsCommand;
