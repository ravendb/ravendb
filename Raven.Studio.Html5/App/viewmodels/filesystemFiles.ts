import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem");
import uploadFileToFilesystemCommand = require("commands/uploadFileToFilesystemCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class filesystemFiles extends viewModelBase {

    uploadQueue = ko.observableArray();
    fileName = ko.observable<File>();

    constructor() {
        super();

        var self = this;

        ko.bindingHandlers["fileUpload"] = {
            init: function (element, valueAccessor) {
            },
            update: function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
                var options = ko.utils.unwrapObservable(valueAccessor());
                var context = <filesystemFiles>viewModel;
                var uploadQueue = ko.utils.unwrapObservable(context.uploadQueue);
                var filesystem = ko.utils.unwrapObservable<filesystem>(bindingContext.$data["activeFilesystem"]);


                if (options) {
                    if (element.files.length) {
                        var file = <File>element.files[0];
                        var guid = system.guid();
                        var item = new uploadItem(guid, file.name, "Queued");
                        //item = { id: guid, fileName: file.name, status: "Queued" };
                        context.uploadQueue.push(item);
                        var indexInQueue = context.uploadQueue.indexOf(uploadItem);

                        new uploadFileToFilesystemCommand(file, guid, filesystem, function (event: any) {
                            if (event.lengthComputable) {
                                var percentComplete = event.loaded / event.total;
                                $(".bar").width(percentComplete);
                                $(".percent").html(percentComplete + "%");
                            }
                        }, true).execute().done(function () {
                                item.status("Uploaded");
                                //context.uploadQueue(uploadQueue);
                            }).fail();

                        context.fileName(null);

                        item.status("Uploading...");
                        //context.uploadQueue(uploadQueue);
                    }
                }
            },
        }
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        //this.collectionToSelectName = args ? args.collection : null;

        //return this.fetchCollections(appUrl.getDatabase());
    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {
            //var documentsUrl = appUrl.forDocuments(null, db);
            //router.navigate(documentsUrl, false);
            //this.fetchCollections(db);
        }
    }
}

class uploadItem {
    id = ko.observable<string>("");
    fileName = ko.observable<string>("");
    public status = ko.observable<string>("");

    constructor(id: string, fileName: string, status: string) {
        this.id(id);
        this.fileName(fileName);
        this.status(status);
    }
}

export = filesystemFiles;