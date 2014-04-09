import system = require("durandal/system");
import router = require("plugins/router"); 
import appUrl = require("common/appUrl");
import uploadFileToFilesystemCommand = require("commands/filesystem/uploadFileToFilesystemCommand");
import filesystem = require("models/filesystem/filesystem");
import uploadItem = require("models/uploadItem");
import viewModelBase = require("viewmodels/viewModelBase");

class filesystemUploadFile extends viewModelBase {

    localStorageKey: string;
    files = ko.observable<File[]>();
    uploadQueue = ko.observableArray<uploadItem>();

    constructor() {
        super();

        this.uploadQueue.subscribe(x => this.updateLocalStorage(x));

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
                            var item = new uploadItem(guid, file.name, "Queued");
                            context.uploadQueue.push(item);

                            new uploadFileToFilesystemCommand(file, guid, filesystem, function (event: any) {
                                if (event.lengthComputable) {
                                    var percentComplete = event.loaded / event.total;
                                    //do something
                                }
                            }, true).execute()
                                .done((x: string) => {
                                    context.updateQueueStatus(x, "Uploaded");
                                })
                                .fail((x: string) => {
                                    context.updateQueueStatus(x, "Failed");
                                });


                            item.status("Uploading...");
                            context.uploadQueue.notifySubscribers();
                        }
                    }

                    context.files(null);
                }
            },
        }
    }

    activate(navigationArgs) {

        this.localStorageKey = "ravenFs-uploadQueue." + this.activeFilesystem().name;

        if (window.localStorage.getItem(this.localStorageKey)) {
            this.uploadQueue(this.parseUploadQueue(window.localStorage.getItem(this.localStorageKey)));
        }
    }

    updateQueueStatus(guid: string, status: string) {
        var items = ko.utils.arrayFilter(this.uploadQueue(), (i: uploadItem) => {
            return i.id() === guid
        });
        if (items) {
            items[0].status(status);
        }

        
    }

    updateLocalStorage(x: uploadItem[]) {
        window.localStorage.setItem(this.localStorageKey, this.stringifyUploadQueue(this.uploadQueue()));
    }

    stringifyUploadQueue(queue: uploadItem[]) : string {
         return ko.toJSON(this.uploadQueue);
    }

    clearUploadQueue() {
        window.localStorage.removeItem(this.localStorageKey);
        this.uploadQueue.removeAll();
    }

    parseUploadQueue(queue: string) : uploadItem[] {
        var stringArray: any[] = JSON.parse(queue);
        var uploadQueue: uploadItem[] = [];

        for (var i = 0; i < stringArray.length; i++) {
            uploadQueue.push(new uploadItem(stringArray[i]["id"], stringArray[i]["fileName"], stringArray[i]["status"]));
        }

        return uploadQueue;
    }

    navigateToFiles() {
        router.navigate(appUrl.forFilesystemFiles(this.activeFilesystem()));
    }
}

export = filesystemUploadFile;