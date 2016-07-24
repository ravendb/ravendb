import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import getFileCommand = require("commands/filesystem/getFileCommand");
import updateFileMetadataCommand = require("commands/filesystem/updateFileMetadataCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import file = require("models/filesystem/file");
import fileMetadata = require("models/filesystem/fileMetadata");
import deleteItems = require("viewmodels/common/deleteItems");
import fileRenameDialog = require("viewmodels/filesystem/files/fileRenameDialog");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class filesystemEditFile extends viewModelBase {

    metadataEditor: AceAjax.Editor;
    fileName = ko.observable<string>();
    file = ko.observable<file>();
    filesystemForEditedFile: filesystem;
    topRecentFiles = ko.computed(() => this.getTopRecentFiles());
    metadata: KnockoutComputed<fileMetadata>;
    fileMetadataEditor: AceAjax.Editor;
    fileMetadataText = ko.observable<string>();
    isBusy = ko.observable(false);
    metaPropsToRestoreOnSave = [];
    isSaveEnabled: KnockoutComputed<boolean>;

    static editFileSelector = "#editFileContainer";
    static recentDocumentsInFilesystem = ko.observableArray<{ filesystemName: string; recentFiles: KnockoutObservableArray<string> }>();

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.fileName.subscribe(x => this.loadFile(x));
        this.isSaveEnabled = ko.pureComputed(() => this.dirtyFlag().isDirty());
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("RJBNGR");
        this.metadata = ko.computed(() => this.file() ? this.file().__metadata : null);
        this.filesystemForEditedFile = appUrl.getFileSystem();
        if (args.id != null) {
            this.appendRecentFile(args.id);
            this.fileName(args.id);
        }
        this.dirtyFlag = new ko.DirtyFlag([this.fileMetadataText]);

        this.metadata.subscribe((meta: fileMetadata) => this.metadataChanged(meta));
    }

    attached() {
        super.attached();
        this.setupKeyboardShortcuts();
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $("#fileMetadataEditor");
        if (editorElement.length > 0) {
            this.metadataEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
    }

        this.focusOnEditor();
    }

    setupKeyboardShortcuts() {
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteFile(), filesystemEditFile.editFileSelector);
    }

    focusOnEditor() {
        this.metadataEditor.focus();
    }

    loadFile(fileName: string) {
        new getFileCommand(this.activeFilesystem(), fileName)
            .execute()
            .done((result: file) => {
                this.file(result);
                this.dirtyFlag().reset();
            });
    }

    navigateToFiles() {
        var filesUrl = appUrl.forFilesystemFiles(this.activeFilesystem());
        router.navigate(filesUrl);
    }

    saveFileMetadata() {
        //the name of the document was changed and we have to save it as a new one
        var meta = JSON.parse(this.fileMetadataText());

        this.metaPropsToRestoreOnSave.forEach(p => meta[p.name] = p.value);

        var saveCommand = new updateFileMetadataCommand(this.fileName(), meta, this.activeFilesystem(), true);
        var saveTask = saveCommand.execute();
        saveTask.done(() => {
            this.loadFile(this.fileName());
        });
    }

    downloadFile() {
        var fs = this.activeFilesystem();
        var fileName = this.fileName();

        var url = appUrl.forResourceQuery(fs) + "/files/" + encodeURIComponent(fileName);
        this.downloader.download(fs, url);
    }

    refreshFile() {
        this.loadFile(this.fileName());
    }

    deleteFile() {
        var file = this.file();
        if (file) {
            var viewModel = new deleteItems([file]);
            viewModel.deletionTask.done(() => {
                var filesUrl = appUrl.forFilesystemFiles(this.activeFilesystem());
                router.navigate(filesUrl);
            });
            app.showDialog(viewModel, filesystemEditFile.editFileSelector);
        }

        this.dirtyFlag().reset(); // Resync Changes
    }

    renameFile() {
        var currentFileName = this.fileName();
        var dialog = new fileRenameDialog(currentFileName, this.activeFilesystem());
        dialog.onExit().done((newName: string) => {
            this.removeFromTopRecentFiles(currentFileName);
            router.navigate(appUrl.forEditFile(newName, this.activeFilesystem()));
        });
        app.showDialog(dialog);
    }

    removeFromTopRecentFiles(fileName: string) {
        var currentFilesystemName = this.activeFilesystem().name;
        var recentFilesForCurFilesystem = filesystemEditFile.recentDocumentsInFilesystem().first(x => x.filesystemName === currentFilesystemName);
        if (recentFilesForCurFilesystem) {
            recentFilesForCurFilesystem.recentFiles.remove(fileName);
        }
    }

    getTopRecentFiles() {
        var currentFilesystemName = this.activeFilesystem().name;
        var recentFilesForCurFilesystem = filesystemEditFile.recentDocumentsInFilesystem().first(x => x.filesystemName === currentFilesystemName);
        if (recentFilesForCurFilesystem) {
            var value = recentFilesForCurFilesystem
                .recentFiles()
                .filter((x: string) => {
                    return x !== this.fileName();
                })
                .slice(0, 5)
                .map((fileId: string) => {
                    return {
                        fileId: fileId,
                        fileUrl: appUrl.forEditFile(fileId, this.activeFilesystem())
                    };
                });
            return value;
        } else {
            return [];
        }
    }

    metadataChanged(meta: fileMetadata) {
        if (meta) {
            //this.metaPropsToRestoreOnSave.length = 0;
            var metaDto = this.metadata().toDto();

            // We don't want to show certain reserved properties in the metadata text area.
            // Remove them from the DTO, restore them on save.
            var metaPropsToRemove = ["Raven-Last-Modified", "Raven-Creation-Date", "Last-Modified", "Creation-Date", "ETag", "RavenFS-Size" ];

            for (var property in metaDto) {
                if (metaDto.hasOwnProperty(property) && metaPropsToRemove.contains(property)) {
                    var value = metaDto[property];
                    if (typeof (value) != "string" && typeof (value) != "number") {
                        this.metaPropsToRestoreOnSave.push({ name: property, value: JSON.stringify(value) });
                    }
                    else {
                        this.metaPropsToRestoreOnSave.push({ name: property, value: metaDto[property].toString() });
                    }
                    delete metaDto[property];
                }
            }

            var metaString = this.stringify(metaDto);
            this.fileMetadataText(metaString);
        }
    }

    appendRecentFile(fileId: string) {
        var existingRecentFilesStore = filesystemEditFile.recentDocumentsInFilesystem.first(x=> x.filesystemName === this.filesystemForEditedFile.name);
        if (existingRecentFilesStore) {
            var existingDocumentInStore = existingRecentFilesStore.recentFiles.first(x=> x === fileId);
            if (!existingDocumentInStore) {
                if (existingRecentFilesStore.recentFiles().length === 5) {
                    existingRecentFilesStore.recentFiles.pop();
                }
                existingRecentFilesStore.recentFiles.unshift(fileId);
            }
        } else {
            filesystemEditFile.recentDocumentsInFilesystem.push({ filesystemName: this.filesystemForEditedFile.name, recentFiles: ko.observableArray([fileId]) });
        }
    }

    private stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }
}

export = filesystemEditFile;
