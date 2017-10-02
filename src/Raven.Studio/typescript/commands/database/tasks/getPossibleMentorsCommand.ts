import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getPossibleMentorsCommand extends commandBase {

    constructor(private dbName: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        const url = endpoints.global.databases.databases;
        const args = {
            name: this.dbName
        };

        return this.query<string[]>(url, args, null, (dbInfo: Raven.Client.ServerWide.Operations.DatabaseInfo) => {
            return dbInfo.NodesTopology
                .Members
                .map(x => x.NodeTag);
        });
    }
}

export = getPossibleMentorsCommand;
