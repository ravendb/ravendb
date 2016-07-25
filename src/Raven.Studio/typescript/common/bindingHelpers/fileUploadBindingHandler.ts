import composition = require("durandal/composition");
import filesystem = require("models/filesystem/filesystem");
import system = require("durandal/system");
import uploadItem = require("models/filesystem/uploadItem");
import uploadFileToFilesystemCommand = require("commands/filesystem/uploadFileToFilesystemCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import uploadQueueHelper = require("common/uploadQueueHelper");

// Usage: <input type="file" data-bind="fileUpload: { files: files, uploads: uploadQueue, success: uploadSuccess.bind($data), fail: uploadFailed.bind($data) }" />
// files: KnockoutObservable<File[]>
// uploads: KnockoutObservableArray<uploadItem>
// success: (i: uploadItem) => void;
// fail: (i: uploadItem) => void;
class fileUploadBindingHandler {
    
    static install() {
        if (!ko.bindingHandlers["fileUpload"]) {
            ko.bindingHandlers["fileUpload"] = new fileUploadBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("fileUpload");
        }
    }
    
    init(element: HTMLInputElement, valueAccessor: any) {
    }

    update(element: HTMLInputElement, valueAccessor: any, allBindingsAccessor: KnockoutAllBindingsAccessor, viewModel: viewModelBase, bindingContext: KnockoutBindingContext) {
        var options: {
            files: KnockoutObservable<FileList>;
            directory: KnockoutObservable<string>;
            uploads: KnockoutObservableArray<uploadItem>;
            before?: () => void;
            success: (i: uploadItem) => void;
            fail: (i: uploadItem) => void;
        } = <any>ko.utils.unwrapObservable(valueAccessor());
        var context = viewModel;
        var filesystem = ko.utils.unwrapObservable<filesystem>(bindingContext.$data["activeFilesystem"]);
        
        if (options) {
            options.files();
            // Access the files observable now so that .update is called whenever it changes.
            if (element.files.length) {
                if (options.before) {
                    options.before();
                }
                var files = element.files;
                for (var i = 0; i < files.length; i++) {
                    var file = files[i];
                    var guid = system.guid();
                    var directory = options.directory() ? options.directory() : ""
                    var item = new uploadItem(guid, directory + "/" + file.name, uploadQueueHelper.queuedStatus, context.activeFilesystem());
                    options.uploads.push(item);
                    
                    new uploadFileToFilesystemCommand(file, directory, guid, filesystem, (e: any) => this.uploadProgressReported(e), true)
                        .execute()
                        .done((x: uploadItem) => options.success(x))
                        .fail((x: uploadItem) => options.fail(x));

                    item.status(uploadQueueHelper.uploadingStatus);
                    options.uploads(uploadQueueHelper.sortUploadQueue(options.uploads()));
                    options.uploads.notifySubscribers(options.uploads());
                }
            }

            options.files(null);
        }
    }

    uploadProgressReported(e: any) {
        if (e.lengthComputable) {
            var percentComplete = e.loaded / e.total;
            //do something
        }
    }
}

export = fileUploadBindingHandler;
