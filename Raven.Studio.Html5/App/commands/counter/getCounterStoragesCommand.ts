import counterStorage = require("models/counter/counterStorage");
import commandBase = require("commands/commandBase");

class getCounterStoragesCommand extends commandBase {
    execute(): JQueryPromise<counterStorage[]> {
        var resultsSelector = (counterStorageNames: string[]) => counterStorageNames.map(n => new counterStorage(n));
        return this.query("/counterStorage/conterStorages", { pageSize: 1024 }, null, resultsSelector);
    }
}

export = getCounterStoragesCommand;