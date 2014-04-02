import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import ace = require("ace/ace");

import pagedList = require("common/pagedList");
import filesystem = require("models/filesystem/filesystem");
//import collection = require("models/filesystemConfigurationKeyCollection");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import getFilesystemConfigurationCommand = require("commands/filesystem/getFilesystemConfigurationCommand");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");

class configuration extends viewModelBase {

    private router = router;

    keys = ko.observableArray<string>();
    allPagedKeyDetails = ko.observable<pagedList>();
    currentKey = ko.observable<string>();

    static containerId = "#settingsContainer";

    static gridSelector = "#filesGrid";

    constructor() {
        super();
    }

    attached() {

        this.activeFilesystem.subscribe(x => {
            this.loadKeys(x); 
            if (this.currentKey() != null)
                this.loadEditorWithKey(x, this.currentKey());       
        }); 

        this.loadKeys(this.activeFilesystem()); 
    }

    activate(args) {
        super.activate(args); 
        if (args.key != null) {
            this.currentKey(args.key);
        }  
       
        this.currentKey.subscribe(x => this.loadEditorWithKey(this.activeFilesystem(), x)); 
    }

    loadKeys(fs: filesystem){
        new getFilesystemConfigurationCommand(fs)
            .execute()
            .done(x => {
                this.keys(x);
                if (this.currentKey() == null)
                    this.currentKey(x.first());                 
            });
    }

    isActive(key: string): boolean{
        return this.currentKey() === key;
    }

    getEditorUrl(key: string): string{
        return appUrl.forFilesystemConfigurationWithKey(this.activeFilesystem(), key);
    }

    loadEditorWithKey(fs: filesystem, key: string) { 
        if (key != null) {
            new getConfigurationByKeyCommand(this.activeFilesystem(), key)
                .execute()
                .done(x => this.handleDocument(x))
                .fail(x => this.navigate(appUrl.forFilesystemConfiguration(fs)));
        }               
    }

    editSelectedKey() {
        var grid = this.getKeyDetailsGrid();
        if (grid) {
            grid.editLastSelectedItem();
        }
    }

    getKeyDetailsGrid(): virtualTable {
        var gridContents = $(configuration.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    private handleDocument(doc: any) {
        var docSettings = this.stringify(doc);
        //this.docEditor.getSession().setValue(docSettings);
    }

    private stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    navigateToEditSettingsDocument() {
        var fs = this.activeFilesystem();
        if (fs && this.currentKey() != null) {
            router.navigate(appUrl.forFilesystemConfigurationWithKey(this.activeFilesystem(), this.currentKey()));
        }
    }
} 

export = configuration;