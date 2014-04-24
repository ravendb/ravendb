import system = require("durandal/system");
import router = require("plugins/router"); 
import appUrl = require("common/appUrl");
import uploadFileToFilesystemCommand = require("commands/filesystem/uploadFileToFilesystemCommand");
import filesystem = require("models/filesystem/filesystem");
import uploadItem = require("models/uploadItem");
import viewModelBase = require("viewmodels/viewModelBase");

class filesystemUploadFile extends viewModelBase {

    localStorageUploadQueueKey: string;
    files = ko.observable<File[]>();
    uploadQueue = ko.observableArray<uploadItem>();

    constructor() {
        super();

        this.uploadQueue.subscribe(x => this.updateLocalStorage(x, this.activeFilesystem()));

        ko.bindingHandlers["fileUpload"] = {
            init: function (element, valueAccessor) {
            },
            update: function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
                var options = ko.utils.unwrapObservable(valueAccessor());
                var context = <filesystemUploadFile>viewModel;
                var filesystem = ko.utils.unwrapObservable<filesystem>(bindingContext.$data["activeFilesystem"]);


                if (options) {
                    if (element.files.length) {
                        var files = <File[]>element.files;
                        for (var i = 0; i < files.length; i++) {
                            var file = files[i];
                            var guid = system.guid();
                            var item = new uploadItem(guid, file.name, "Queued", context.activeFilesystem());
                            context.uploadQueue.push(item);

                            new uploadFileToFilesystemCommand(file, guid, filesystem, function (event: any) {
                                if (event.lengthComputable) {
                                    var percentComplete = event.loaded / event.total;
                                    //do something
                                }
                            }, true).execute()
                                .done((x: uploadItem) => {
                                    ko.postbox.publish("UploadFileStatusChanged", x);
                                    context.updateQueueStatus(x.id(), "Uploaded", context.uploadQueue());
                                })
                                .fail((x: uploadItem) => {
                                    ko.postbox.publish("UploadFileStatusChanged", x);
                                    context.updateQueueStatus(x.id(), "Failed", context.uploadQueue());
                                });


                            item.status("Uploading...");
                            context.uploadQueue.notifySubscribers(context.uploadQueue());
                        }
                    }

                    context.files(null);
                }
            },
        }
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
}

export = filesystemUploadFile;