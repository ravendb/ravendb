import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class fsCheckSufficientDiskSpaceCommand extends commandBase {

    constructor(private fileSize: number, private filesystem: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        var args = {
            fileSize: this.fileSize
        }
        var url = "/studio-tasks/check-sufficient-diskspace";
        var checkTask = this.query(url, args, this.filesystem, undefined, { dataType: undefined });

        return checkTask;
    }
}

export = fsCheckSufficientDiskSpaceCommand; 