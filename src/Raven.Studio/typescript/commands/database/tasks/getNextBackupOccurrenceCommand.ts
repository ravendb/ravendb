import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getNextBackupOccurrenceCommand extends commandBase {
    constructor(private backupFrequency: string) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.System.NextBackupOccurrence> {
        const url = endpoints.global.backupDatabase.periodicBackupNextBackupOccurrence +
            this.urlEncodeArgs({ backupFrequency: this.backupFrequency });

        return this.query(url, null);
    }
}

export = getNextBackupOccurrenceCommand; 

