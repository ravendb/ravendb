import system = require("durandal/system");
import router = require("plugins/router"); 
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import uploadItem = require("models/uploadItem");
import viewModelBase = require("viewmodels/viewModelBase");
import fileUploadBindingHandler = require("common/fileUploadBindingHandler");

class filesystemUploadFile extends viewModelBase {

    localStorageUploadQueueKey: string;
    files = ko.observable<File[]>();
    uploadQueue = ko.observableArray<uploadItem>();

    constructor() {
        super();
        
        this.uploadQueue.subscribe(x => this.updateLocalStorage(x, this.activeFilesystem()));
        fileUploadBindingHandler.install();
    }

    activate(navigationArgs) {
        super.activate(navigationArgs);

        var storageKeyForFs = this.localStorageUploadQueueKey + this.activeFilesystem().name;
        if (window.localStorage.getItem(storageKeyForFs)) {
            this.uploadQueue(
                this.parseUploadQueue(
                    window.localStorage.getItem(storageKeyForFs), this.activeFilesystem()));
        }
    }

    clearUploadQueue() {
        window.localStorage.removeItem(this.localStorageUploadQueueKey + this.activeFilesystem().name);
        this.uploadQueue.removeAll();
    }

    navigateToFiles() {
        router.navigate(appUrl.forFilesystemFiles(this.activeFilesystem()));
    }

    uploadSuccess(x: uploadItem) {
        ko.postbox.publish("UploadFileStatusChanged", x);
        this.updateQueueStatus(x.id(), "Uploaded", this.uploadQueue());
    }

    uploadFailed(x: uploadItem) {
        ko.postbox.publish("UploadFileStatusChanged", x);
        this.updateQueueStatus(x.id(), "Failed", this.uploadQueue());
    }
}

export = filesystemUploadFile;