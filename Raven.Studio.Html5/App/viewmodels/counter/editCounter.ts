import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import counterChange = require("models/counter/counterChange");
import counter = require("models/counter/counter");
import counterSummary = require("models/counter/counterSummary");
import editCounterDialog = require("viewmodels/counter/editCounterDialog");
import getCounterCommand = require("commands/counter/getCounterCommand");
import updateCounterCommand = require("commands/counter/updateCounterCommand");
import resetCounterCommand = require("commands/counter/resetCounterCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteItems = require("viewmodels/common/deleteItems");

class editCounter extends viewModelBase {

	groupName = ko.observable<string>();
	counterName = ko.observable<string>();
	groupLink = ko.observable<string>("123");
	counter = ko.observable<counter>();
	isLoading = ko.observable<boolean>();
    //topRecentCounters = ko.computed(() => this.getTopRecentCounters());
    isBusy = ko.observable(false);

    static container = "#editCounterContainer";
    static recentDocumentsInFilesystem = ko.observableArray<{ filesystemName: string; recentFiles: KnockoutObservableArray<string> }>();

    constructor() {
	    super();
    }

	canActivate(args) {
		super.canActivate(args);

		var deffered = $.Deferred();
		if (!args.groupName || !args.counterName) {
			messagePublisher.reportError("Can't find group name or counter name in query string!");
			deffered.resolve({ redirect: appUrl.forCounterStorageCounters(null, this.activeCounterStorage()) });
		}
		this.load(args.groupName, args.counterName)
			.done(() => deffered.resolve({ can: true }))
			.fail(() => {
				messagePublisher.reportError("Can't find counter!");
				deffered.resolve({ redirect: appUrl.forCounterStorageCounters(null, this.activeCounterStorage()) });
			});

		return deffered;
	}

    /*activate(args) {
        super.activate(args);
        /*this.metadata = ko.computed(() => this.file() ? this.file().__metadata : null);
        this.filesystemForEditedFile = appUrl.getFileSystem();
        if (args.id != null) {
            this.appendRecentFile(args.id);
            this.fileName(args.id);
        }

        this.metadata.subscribe((meta: fileMetadata) => this.metadataChanged(meta));#1#
    }*/

    attached() {
        this.setupKeyboardShortcuts();
    }

    setupKeyboardShortcuts() {
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteCounter(), editCounter.container);
	}

    load(groupName: string, counterName: string) {
	    this.isLoading(true);
        return new getCounterCommand(this.activeCounterStorage(), groupName, counterName)
            .execute()
            .done((result: counter) => {
		        this.groupName(groupName);
		        this.counterName(counterName);
		        this.counter(result);
				this.isLoading(false);
	        });
    }

    navigateToFiles() {
        var filesUrl = appUrl.forFilesystemFiles(this.activeFilesystem());
        router.navigate(filesUrl);
    }

	refresh() {
		this.load(this.groupName(), this.counterName());
	}

	change() {
        var dto = {
            CurrentValue: this.counter().total(),
            Group: this.groupName(),
            CounterName: this.counterName(),
            Delta: 0
        };
        var change = new counterChange(dto);
        var counterChangeVm = new editCounterDialog(change);
        counterChangeVm.updateTask.done((change: counterChange, isNew: boolean) => {
            var counterCommand = new updateCounterCommand(this.activeCounterStorage(), change.group(), change.counterName(), change.delta(), isNew);
	        var execute = counterCommand.execute();
			execute.done(() => this.refresh());
        });
        app.showDialog(counterChangeVm);
    }

	reset() {
        var confirmation = this.confirmationMessage("Reset Counter", "Are you sure that you want to reset the counter?");
        confirmation.done(() => {
            var resetCommand = new resetCounterCommand(this.activeCounterStorage(), this.groupName(), this.counterName());
            var execute = resetCommand.execute();
			execute.done(() => this.refresh());
        });
    }

	deleteCounter() {
		var summary: counterSummary = new counterSummary({
			GroupName: this.groupName(),
			CounterName: this.counterName(),
			Total: this.counter().total()
		});
        var viewModel = new deleteItems([summary]);
        viewModel.deletionTask.done(() => {
            var countersUrl = appUrl.forCounterStorageCounters(null, this.activeCounterStorage());
            router.navigate(countersUrl);
        });
        app.showDialog(viewModel, editCounter.container);
    }

    /*saveFileMetadata() {
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
    }*/

    /*downloadFile() {
        var url = appUrl.forResourceQuery(this.activeFilesystem()) + "/files/" + this.fileName();
        window.location.assign(url);
    }*/

	/*removeFromTopRecentFiles(fileName: string) {
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
    }*/

    /*metadataChanged(meta: fileMetadata) {
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
    }*/

    /*appendRecentFile(fileId: string) {

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

    }*/
}

export = editCounter;