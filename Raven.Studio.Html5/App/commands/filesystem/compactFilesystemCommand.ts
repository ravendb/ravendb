import commandBase = require("commands/commandBase");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import appUrl = require("common/appUrl");
import getOperationStatusCommand = require('commands/getOperationStatusCommand');

class compactFilesystemCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        var url = '/admin/fs/compact' + this.urlEncodeArgs({ filesystem: this.fs.name });
        this.post(url, null, appUrl.getSystemDatabase())
            .done((result: operationIdDto) => this.monitorCompact(promise, result.OperationId))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to compact filesystem!", response.responseText, response.statusText);
                promise.reject();
            });
        return promise;
    }

    private monitorCompact(parentPromise: JQueryDeferred<any>, operationId: number) {
        new getOperationStatusCommand(appUrl.getSystemDatabase(), operationId)
            .execute()
            .done((result: operationStatusDto) => {
                if (result.Completed) {
                    if (result.Faulted) {
                        this.reportError("Failed to compact filesystem!", result.State.Error);
                        parentPromise.reject();
                    } else {
                        this.reportSuccess("Compact completed");
                        parentPromise.resolve();
                    }
                } else {
                    setTimeout(() => this.monitorCompact(parentPromise, operationId), 500);
                }
            });
    }
}

export = compactFilesystemCommand;