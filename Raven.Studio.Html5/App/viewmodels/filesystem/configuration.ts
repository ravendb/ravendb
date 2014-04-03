import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

import pagedList = require("common/pagedList");
import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import getConfigurationCommand = require("commands/filesystem/getConfigurationCommand");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");
import configurationKey = require("models/filesystem/configurationKey");
import customColumns = require('models/customColumns');

class configuration extends viewModelBase {

    private router = router;

    keys = ko.observableArray<configurationKey>();
    selectedKey = ko.observable<configurationKey>().subscribeTo("ActivateConfigurationKey").distinctUntilChanged();
    keyDetails = ko.observable<Array<Pair<string, string[]>>>();
    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    currentKey = ko.observable<configurationKey>();

    static gridSelector = "#keyDetailsGrid";

    constructor() {
        super();
        this.selectedKey.subscribe(k => this.selectedKeyChanged(k));
    }

    attached() {

        this.activeFilesystem.subscribe(x => {
            this.loadKeys(x);     
        }); 

        this.loadKeys(this.activeFilesystem()); 
    }

    loadKeys(fs: filesystem){
        new getConfigurationCommand(fs)
            .execute()
            .done(x => {
                this.keys(x);          
            });
    }

    selectKey(key: configurationKey) {
        key.activate();
        router.navigate(appUrl.forFilesystemConfigurationWithKey(this.activeFilesystem(), key.key));
    }

    selectedKeyChanged(selected: configurationKey) {
        if (selected) {
            selected.getValues().done(x => {
                this.keyDetails(x);
            });
            this.currentKey(selected);
        }
    }

    getKeyDetailsGrid(): virtualTable {
        var gridContents = $(configuration.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

} 

export = configuration;