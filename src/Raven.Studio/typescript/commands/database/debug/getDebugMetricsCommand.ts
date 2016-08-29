import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class getDebugMetricsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<statusDebugMetricsDto> {
        var url = this.getQueryUrlFragment();
        return this.query<statusDebugMetricsDto>(url, null, this.db);
    }

    getQueryUrl(): string {
        return appUrl.forResourceQuery(this.db) + this.getQueryUrlFragment();
    }

    private getQueryUrlFragment(): string {
        return "/debug/metrics";//TODO: use endpoints
    }
}

export = getDebugMetricsCommand;
