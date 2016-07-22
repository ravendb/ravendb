import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import changesContext = require("common/changesContext");
import resolveConflict = require("viewmodels/filesystem/synchronization/resolveConflict");
import customColumns = require("models/database/documents/customColumns");
import customColumnParams = require('models/database/documents/customColumnParams');
import virtualTable = require("widgets/virtualTable/viewModel");

import filesystem = require("models/filesystem/filesystem");
import changeSubscription = require("common/changeSubscription");
import pagedList = require("common/pagedList");

import getFilesConflictsCommand = require("commands/filesystem/getFilesConflictsCommand");
import resolveConflictCommand = require("commands/filesystem/resolveConflictCommand");
import resolveConflictsCommand = require("commands/filesystem/resolveConflictsCommand");

class synchronizationConflicts extends viewModelBase {

    static gridSelector = "#synchronizationConflictsGrid";

    conflictStatus = {
        detected: "Detected",
        resolved: "Resolved"
    };

    currentConflictsPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();
    currentColumns = ko.observable(customColumns.empty());
    conflictsSelection: KnockoutComputed<checkbox>;
    hasAnyConflictsSelected: KnockoutComputed<boolean>;
    hasAllConflictsSelected: KnockoutComputed<boolean>;
    conflictsCount: KnockoutComputed<number>;
    isAnyConflictsAutoSelected = ko.observable<boolean>(false);
    isAllConflictsAutoSelected = ko.observable<boolean>(false);
    selectedConflictsText: KnockoutComputed<string>;

    constructor() {
        super();

        this.conflictsCount = ko.computed(() => {
            if (!!this.currentConflictsPagedItems()) {
                var p: pagedList = this.currentConflictsPagedItems();
                return p.totalResultCount();
            }
            return 0;
        });

        this.selectedConflictsText = ko.computed(() => {
            if (!!this.selectedDocumentIndices()) {
                var documentsText = "conflict";
                if (this.selectedDocumentIndices().length !== 1) {
                    documentsText += "s";
                }
                return documentsText;
            }
            return "";
        });

        this.hasAnyConflictsSelected = ko.computed(() => this.selectedDocumentIndices().length > 0);

        this.hasAllConflictsSelected = ko.computed(() => {
            var filesCount = this.conflictsCount();
            return filesCount > 0 && filesCount === this.selectedDocumentIndices().length;
        });

        this.conflictsSelection = ko.computed(() => {
            var selected = this.selectedDocumentIndices();
            if (this.hasAllConflictsSelected()) {
                return checkbox.Checked;
            }
            if (selected.length > 0) {
                return checkbox.SomeChecked;
            }
            return checkbox.UnChecked;
        });
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("NMDELS");

        this.currentColumns().columns([
            new customColumnParams({ Header: "File Name", Binding: "fileName", DefaultWidth: 400 }),
            new customColumnParams({ Header: "Remote Server Url", Binding: "remoteServerUrl", DefaultWidth: 400 }),
            new customColumnParams({ Header: "Status", Binding: "status", DefaultWidth: 320 })
        ]);
        this.currentColumns().customMode(true);

        this.fetchConflicts(this.activeFilesystem());
    }

    createNotifications(): Array<changeSubscription> {
        return [changesContext.currentResourceChangesApi().watchFsConflicts((e: synchronizationConflictNotification) => this.processFsConflicts(e)) ];
    }

    private processFsConflicts(e: synchronizationConflictNotification) {
        switch (e.Status) {
            case this.conflictStatus.detected:
            case this.conflictStatus.resolved:
                this.fetchConflicts(this.activeFilesystem());
            break;
        }
    }

    private fetchConflicts(fs: filesystem) {
        this.currentConflictsPagedItems(this.createPagedList(fs));
    }

    private createPagedList(fs: filesystem): pagedList {
        var fetcher = (skip: number, take: number) => new getFilesConflictsCommand(fs, skip, take).execute();
        return new pagedList(fetcher);
    }

    selectedConflicts(): string[] {
        return this.getDocumentsGrid().getSelectedItems().map(x => x.getId());
    }

    private getDocumentsGrid(): virtualTable {
        var gridContents = $(synchronizationConflicts.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    resolveWithLocalVersion() {
        var message: string;
        if (this.hasAllConflictsSelected()) {
            message = "Are you sure you want to resolve all conflicts by choosing the local version?";
        } else if (this.selectedConflicts().length === 1) {
            message = "Are you sure you want to resolve the conflict for file <b>" +
                this.selectedConflicts()[0] +
                "</b> by choosing the local version?";
        } else {
            message = "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the local version?";
        }

        var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with local");
        resolveConflictViewModel
            .resolveTask
            .done(() => {
                var fs = this.activeFilesystem();

                if (this.hasAllConflictsSelected()) {
                    new resolveConflictsCommand(2, fs)
                        .execute()
                        .done(() => this.fetchConflicts(this.activeFilesystem()));
                } else {
                    var selectedConflicts = this.selectedConflicts();

                    for (var i = 0; i < selectedConflicts.length; i++) {
                        var conflict = selectedConflicts[i];
                        new resolveConflictCommand(conflict, 2, fs)
                            .execute();
                    }
                }

                this.selectNone();
            });
        app.showDialog(resolveConflictViewModel);
    }

    resolveWithRemoteVersion() {
        var message: string;
        if (this.hasAllConflictsSelected()) {
            message = 'Are you sure you want to resolve all conflicts by choosing the remote version?';
        } else if (this.selectedConflicts().length === 1) {
            message = "Are you sure you want to resolve the conflict for file <b>" +
                this.selectedConflicts()[0] +
                "</b> by choosing the remote version?";
        } else {
            message = "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the remote version?";
        }

        var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with remote");
        resolveConflictViewModel
            .resolveTask
            .done(() => {
                var fs = this.activeFilesystem();

                if (this.hasAllConflictsSelected()) {
                    new resolveConflictsCommand(1, fs)
                        .execute()
                        .done(() => this.fetchConflicts(this.activeFilesystem()));
                } else {
                    for (var i = 0; i < this.selectedConflicts().length; i++) {
                        var conflict = this.selectedConflicts()[i];
                        new resolveConflictCommand(conflict, 1, fs).execute()
                            .done(() => {
                                this.fetchConflicts(this.activeFilesystem());
                            });
                    }
                }

                this.selectNone();
            });
        app.showDialog(resolveConflictViewModel);
    }

    toggleSelectAll() {
        var conflictsGrid = this.getDocumentsGrid();
        if (!!conflictsGrid) {
            if (this.hasAnyConflictsSelected()) {
                conflictsGrid.selectNone();
            } else {
                conflictsGrid.selectSome();
                this.isAnyConflictsAutoSelected(this.hasAllConflictsSelected() === false);
            }
        }
    }

     selectAll() {
        var conflictsGrid = this.getDocumentsGrid();
        if (!!conflictsGrid && !!this.currentConflictsPagedItems()) {
            var p: pagedList = this.currentConflictsPagedItems();
            conflictsGrid.selectAll(p.totalResultCount());
        }
    }

    selectNone() {
        var conflictsGrid = this.getDocumentsGrid();
        if (!!conflictsGrid) {
            conflictsGrid.selectNone();
        }
    }

}

export = synchronizationConflicts;
