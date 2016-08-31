import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class adminLogsConfigureCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private ownerDb: database, private logConfig: adminLogsConfigEntryDto[], private eventsId: string) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<any> {
        var args = {
            'watch-category': $.map(this.logConfig, item => item.category + ":" + item.level + ":" + (item.includeStackTrace ? "watch-stack" : "no-watch-stack")),
            id: this.eventsId
        };

        var url = "/admin/logs/configure" + this.urlEncodeArgs(args);//TODO: use endpoints
        return this.query<any>(url, null, this.ownerDb);
    }
}

export = adminLogsConfigureCommand;
