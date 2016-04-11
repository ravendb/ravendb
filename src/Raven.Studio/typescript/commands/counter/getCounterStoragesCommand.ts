import counterStorage = require("models/counter/counterStorage");
import commandBase = require("commands/commandBase");

class getCounterStoragesCommand extends commandBase {
    execute(): JQueryPromise<counterStorage[]> {
        var args = {
            pageSize: 1024,
            getAdditionalData: true
        };
        var url = "/cs";

        var resultsSelector = (counterStorages: counterStorageDto[]) => 
            counterStorages.map((cs: counterStorageDto) => new counterStorage(cs.Name, cs.IsAdminCurrentTenant, cs.Disabled));
        return this.query(url, args, null, resultsSelector);
    }
}

export = getCounterStoragesCommand;
