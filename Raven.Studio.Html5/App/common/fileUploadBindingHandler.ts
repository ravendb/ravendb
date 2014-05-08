import composition = require("durandal/composition");
import filesystem = require("models/filesystem/filesystem");
import system = require("durandal/system");
import uploadItem = require("models/uploadItem");
import uploadFileToFilesystemCommand = require("commands/filesystem/uploadFileToFilesystemCommand");
import viewModelBase = require("viewmodels/viewModelBase");

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
    
    init(element, valueAccessor) {
    }

    update(element: HTMLInputElement, valueAccessor, allBindingsAccessor, viewModel: viewModelBase, bindingContext) {
        var options: {
            files: KnockoutObservable<File[]>;
            uploads: KnockoutObservableArray<uploadItem>;
            success: (i: uploadItem) => void;
            fail: (i: uploadItem) => void;
        } = <any>ko.utils.unwrapObservable(valueAccessor());
        var context = viewModel;
        var filesystem = ko.utils.unwrapObservable<filesystem>(bindingContext.$data["activeFilesystem"]);
        
        if (options) {
            if (element.files.length) {
                var files = element.files;
                for (var i = 0; i < files.length; i++) {
                    var file = files[i];
                    var guid = system.guid();
                    var item = new uploadItem(guid, file.name, "Queued", context.activeFilesystem());
                    options.uploads.push(item);

                    new uploadFileToFilesystemCommand(file, guid, filesystem, (e: any) => this.uploadProgressReported(e), true)
                        .execute()
                        .done((x: uploadItem) => options.success(x))
                        .fail((x: uploadItem) => options.fail(x));

                    item.status("Uploading...");
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