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
    selectedKeyValue = ko.observable<Pair<string, string>>();
    currentKey = ko.observable<configurationKey>();

    constructor() {
        super();
        this.selectedKey.subscribe(k => this.selectedKeyChanged(k));
    }

    canActivate(args: any) {
        return true;
    }

    attached() {

        this.activeFilesystem.subscribe(x => {
            this.loadKeys(x);     
        });
        
        (<any>$('.keys-collection')).contextmenu({
            target: '#keys-context-menu'
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

                //we collapse the dictionary into a flattened value pair array to show in the UI.
                var nameValueCollection = new Array<Pair<string, string>>();
                
                for (var i = 0; i < x.length; i++) {
                    var pair = x[i];
                    var name = pair.item1;
                    for (var j = 0; j < pair.item2.length; j++) {
                        var value = pair.item2[j];
                        nameValueCollection.push(new Pair(name, value));
                    }
                }

                if (nameValueCollection.length == 0)
                    nameValueCollection.push(new Pair("", ""));   

                this.keyValues(nameValueCollection);
            });

            this.currentKey(selected);
        }
    }

    selectKeyValue(selection: Pair<string, string>) {
        this.selectedKeyValue(selection);
    }

    save() {

        var args: { [name: string]: string[]; } = {};
        var values = this.keyValues();

        for (var i = 0; i < values.length; i++) {

            var key = values[i].item1;
            if (key == null || key == "")
                continue;

            var value = values[i].item2;
            if (value == null || value == "")
                continue;

            if (args[key] == null) {
                args[key] = [value];
            }
            else {
                args[key].push(value);
            }
        }

        new saveConfigurationCommand(this.activeFilesystem(), this.currentKey(), args).execute();
    }

    deleteKey() {
        throw new Error("Not Implemented");
    }

    addKeyValue() {
        this.keyValues.push(new Pair("", ""));
    }

    removeKeyValue(keyValue) {

        if (keyValue) {
            this.keyValues.remove(keyValue);
        }

        if (this.keyValues.length == 0) {
            this.keyValues.push(new Pair("", ""));
        }
    }
} 

export = configuration;