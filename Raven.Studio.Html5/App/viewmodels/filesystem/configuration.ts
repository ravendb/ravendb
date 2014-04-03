import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import getConfigurationCommand = require("commands/filesystem/getConfigurationCommand");
import saveConfigurationCommand = require("commands/filesystem/saveConfigurationCommand");
import configurationKey = require("models/filesystem/configurationKey");

class configuration extends viewModelBase {

    private router = router;

    keys = ko.observableArray<configurationKey>();
    selectedKey = ko.observable<configurationKey>().subscribeTo("ActivateConfigurationKey").distinctUntilChanged();
    keyValues = ko.observableArray<Pair<string, string>>();
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

                var nameValueCollection = new Array<Pair<string, string>>();
                
                for (var i = 0; i < x.length; i++) {
                    var pair = x[i];
                    var name = pair.item1;
                    for (var j = 0; j < pair.item2.length; j++) {
                        var value = pair.item2[j];
                        nameValueCollection.push(new Pair(name, value));
                    }
                }

                this.keyValues(nameValueCollection);
            });

            this.currentKey(selected);
        }
    }

    save() {

        var args: { [name: string]: string[]; } = {};

        new saveConfigurationCommand(this.activeFilesystem(), this.currentKey(), this.keyValues()).execute();
    }

    addKeyValue() {
        this.keyValues.push(new Pair("", ""));
    }

} 

export = configuration;