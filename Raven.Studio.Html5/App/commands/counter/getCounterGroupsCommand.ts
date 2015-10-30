import commandBase = require("commands/commandBase");
import counterGroup = require("models/counter/counterGroup");
import counterStorage = require("models/counter/counterStorage");

class getCounterGroupsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private cs: counterStorage, private skip: number, private take: number) {
        super();

    }

    execute(): JQueryPromise<counterGroup[]> {
        var selector = (groups: counterGroupDto[]) => groups.map((g: counterGroupDto) => counterGroup.fromDto(g, this.cs));

        var args = {
            start: this.skip,
            pageSize: this.take
        };

        return this.query("/groups", args, this.cs, selector);
    }
}

export = getCounterGroupsCommand; 
