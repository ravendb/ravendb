import commandBase = require("commands/commandBase");

class getResourceDrives extends commandBase {
    
    constructor(private name: string, private type: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var args = {
            type: this.type,
            name: this.name
        };
        var url = "/debug/resource-drives";

        return this.query(url, args);
    }
}

export = getResourceDrives;
