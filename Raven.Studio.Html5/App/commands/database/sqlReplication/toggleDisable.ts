import commandBase = require("commands/commandBase");

class resetSqlReplicationCommand extends commandBase {
    constructor(private db, private disable: boolean) {
        super();
    }

    execute() {
        var args = {
	         disable: this.disable
        };
        var url = "/studio-tasks/sql-replication-toggle-disable" + super.urlEncodeArgs(args);

	    return this.post(url, null, this.db)
		    .fail((result: JQueryXHR) => {
                var action = this.disable ? "disable" : "enable";
			    this.reportError("Failed to " + action + " SQL Replications", result.responseText, result.statusText);
		    });
    }
}
export = resetSqlReplicationCommand;
