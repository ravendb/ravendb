import commandBase = require("commands/commandBase");
import counterGroup = require("models/counter/counterGroup");
import counterStorage = require("models/counter/counterStorage");

class getCounterGroupsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private storage: counterStorage) {
        super();

    }

    execute(): JQueryPromise<counterGroup[]> {
        var selector = (groups: counterGroupDto[]) => groups.map(g => new counterGroup(g));
        return this.query("/groups", null, this.storage, selector);
    }
}

export = getCounterGroupsCommand; 