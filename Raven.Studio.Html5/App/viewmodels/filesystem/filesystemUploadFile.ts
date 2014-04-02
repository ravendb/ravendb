import system = require("durandal/system");
import router = require("plugins/router"); 
import appUrl = require("common/appUrl");
import uploadFileToFilesystemCommand = require("commands/filesystem/uploadFileToFilesystemCommand");
import filesystem = require("models/filesystem/filesystem");
import uploadItem = require("models/uploadItem");
import viewModelBase = require("viewmodels/viewModelBase");

class filesystemUploadFile extends viewModelBase {

    fileName = ko.observable<File>();
    uploadQueue = ko.observableArray();

    constructor() {
        super();
    }

    activate(navigationArgs) {
        ko.bindingHandlers["fileUpload"] = {
            init: function (element, valueAccessor) {
            },
            update: function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
                var options = ko.utils.unwrapObservable(valueAccessor());
                var context = <filesystemUploadFile>viewModel;
                var filesystem = ko.utils.unwrapObservable<filesystem>(bindingContext.$data["activeFilesystem"]);


                if (options) {
                    if (element.files.length) {
                        var file = <File>element.files[0];
                        var guid = system.guid();
                        var item = new uploadItem(guid, file.name, "Queued");
                        context.uploadQueue.push(item);

                        new uploadFileToFilesystemCommand(file, guid, filesystem, function (event: any) {
                            if (event.lengthComputable) {
                                var percentComplete = event.loaded / event.total;
                                //do something
                            }
                        }, true).execute()
                            .done(function () { item.status("Uploaded"); })
                            .fail(function () { item.status("Failed"); });

                        context.fileName(null);

                        item.status("Uploading...");
                    }
                }
            },
        }
    }

    navigateToFiles() {
        router.navigate(appUrl.forFilesystemFiles(this.activeFilesystem()));
    }
}

export = filesystemUploadFile;