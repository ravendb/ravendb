import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

// Operator only: device level state is shared by every database on the disk
class getStorageIoCommand extends commandBase {
    execute(): JQueryPromise<Raven.Server.Storage.StorageIoReport> {
        const url = endpoints.global.adminStorageIo.adminDebugStorageIo;
        return this.query<Raven.Server.Storage.StorageIoReport>(url, null).fail((response: JQueryXHR) =>
            this.reportError("Failed to load the storage I/O state", response.responseText, response.statusText)
        );
    }
}

export = getStorageIoCommand;
