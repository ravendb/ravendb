import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getNextBackupOccurrenceCommand extends commandBase {
    constructor(private date: string, private backupFrequency: string) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.System.NextBackupOccurrence> {
        const url = endpoints.global.databases.periodicBackupNextBackupOccurrence +
            this.urlEncodeArgs({ date: this.date, backupFrequency: this.backupFrequency });

        return this.query(url, null);
    }
}

export = getNextBackupOccurrenceCommand; 

