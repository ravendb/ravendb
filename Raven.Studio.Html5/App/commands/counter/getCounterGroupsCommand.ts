import commandBase = require("commands/commandBase");
import database = require("models/database");
import counterGroup = require("models/counter/counterGroup");
import appUrl = require("common/appUrl");

class getCounterGroupsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor() {
        super();

    }

    execute(): JQueryPromise<counterGroup[]> {
        var selector = (names: string[]) => names.map(n => new counterGroup(n));
       return this.query("/counters/test/groups", null, appUrl.getSystemDatabase(), selector);
    }
}

export = getCounterGroupsCommand; 