import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import ace = require("ace/ace");

import filesystem = require("models/filesystem/filesystem");
import getFileCommand = require("commands/filesystem/getFileCommand");
import updateFileMetadataCommand = require("commands/filesystem/updateFileMetadataCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import file = require("models/filesystem/file");
import fileMetadata = require("models/filesystem/fileMetadata");
import deleteItems = require("viewmodels/deleteItems");
import fileRenameDialog = require("viewmodels/filesystem/fileRenameDialog");

class filesystemEditFile extends viewModelBase {

    fileName = ko.observable<string>();
    file = ko.observable<file>();
    filesystemForEditedFile: filesystem;
    topRecentFiles = ko.computed(() => this.getTopRecentFiles());
    metadata: KnockoutComputed<fileMetadata>;
    fileMetadataEditor: AceAjax.Editor;
    fileMetadataText = ko.observable<string>();
    isBusy = ko.observable(false);
    metaPropsToRestoreOnSave = [];

    static editFileSelector = "#editFileContainer";
    static recentDocumentsInFilesystem = ko.observableArray<{ filesystemName: string; recentFiles: KnockoutObservableArray<string> }>();

    constructor() {
        super();

        // When we programmatically change the document text or meta text, push it into the editor.
        this.fileMetadataText.subscribe(() => this.updateFileEditorText());
        this.fileName.subscribe(x => this.loadFile(x));
    }

    activate(args) {
        super.activate(args);
        this.metadata = ko.computed(() => this.file() ? this.file().__metadata : null);
        this.filesystemForEditedFile = appUrl.getFileSystem();
        if (args.id != null) {
            this.appendRecentFile(args.id);
            this.fileName(args.id);
        }

        this.metadata.subscribe((meta: fileMetadata) => this.metadataChanged(meta));
    }

    // Called when the view is attached to the DOM.
    attached() {
        this.initializeFileEditor();
        this.setupKeyboardShortcuts();
        this.focusOnEditor();
    }

    setupKeyboardShortcuts() {
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteFile(), filesystemEditFile.editFileSelector);
    }

    initializeFileEditor() {
        // Startup the Ace editor with JSON syntax highlighting.
        // TODO: Just use the simple binding handler instead.
        this.fileMetadataEditor = ace.edit("fileMetadataEditor");
        this.fileMetadataEditor.setTheme("ace/theme/xcode");
        this.fileMetadataEditor.setFontSize("16px");
        this.fileMetadataEditor.getSession().setMode("ace/mode/json");
        $("#fileMetadataEditor").on('blur', ".ace_text-input", () => this.storeFileEditorTextIntoObservable());
        this.updateFileEditorText();
    }

    focusOnEditor() {
        this.fileMetadataEditor.focus();
    }

    updateFileEditorText() {
        if (this.fileMetadataEditor) {
            this.fileMetadataEditor.getSession().setValue(this.fileMetadataText());
        }
    }

    storeFileEditorTextIntoObservable() {
        if (this.fileMetadataEditor) {
            var editorText = this.fileMetadataEditor.getSession().getValue();
            this.fileMetadataText(editorText);
        }
    }

    loadFile(fileName: string) {
        new getFileCommand(this.activeFilesystem(), fileName)
            .execute()
            .done((result: file) => this.file(result));
    }

    navigateToFiles() {
        var filesUrl = appUrl.forFilesystemFiles(this.activeFilesystem());
        router.navigate(filesUrl);
    }

    saveFileMetadata() {
        //the name of the document was changed and we have to save it as a new one
        var meta = JSON.parse(this.fileMetadataText());
        var currentDocumentId = this.fileName();

        this.metaPropsToRestoreOnSave.forEach(p => meta[p.name] = p.value);

        var saveCommand = new updateFileMetadataCommand(this.fileName(), meta, this.activeFilesystem(), true);
        var saveTask = saveCommand.execute();
        saveTask.done(() => {
            this.dirtyFlag().reset(); // Resync Changes

            this.loadFile(this.fileName());
        });
    }

    downloadFile() {
        var url = appUrl.forResourceQuery(this.activeFilesystem()) + "/files/" + this.fileName();
        window.location.assign(url);
    }

    refreshFile() {
        this.loadFile(this.fileName());
    }

    saveInObservable() { //TODO: remove this and use ace binding handler
        this.storeFileEditorTextIntoObservable();
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
        var dialog = new fileRenameDialog(this.fileName(), this.activeFilesystem());
        dialog.onExit().done((newName: string) => {
            router.navigate(appUrl.forEditFile(newName, this.activeFilesystem()));
        });
        app.showDialog(dialog);
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

        var existingRecentFilesStore = filesystemEditFile.recentDocumentsInFilesystem.first(x=> x.filesystemName == this.filesystemForEditedFile.name);
        if (existingRecentFilesStore) {
            var existingDocumentInStore = existingRecentFilesStore.recentFiles.first(x=> x === fileId);
            if (!existingDocumentInStore) {
                if (existingRecentFilesStore.recentFiles().length == 5) {
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