import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class getFoldersStartsWithCommand extends commandBase {

    constructor(private fs: filesystem, private skip : number, private take: number, private directory?: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {

        var url = "/folders/Subdirectories";
        var args :any = {
            start: this.skip,
            pageSize: this.take,
            startsWith: true
        }

        if (this.directory) {
            args.directory = this.directory;
        }
        
        return this.query<string[]>(url, args, this.fs);
    }
}

export = getFoldersStartsWithCommand;
