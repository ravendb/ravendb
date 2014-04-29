import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import ace = require("ace/ace");

import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import getConfigurationCommand = require("commands/filesystem/getConfigurationCommand");
import saveConfigurationCommand = require("commands/filesystem/saveConfigurationCommand");
import configurationKey = require("models/filesystem/configurationKey");

class configuration extends viewModelBase {

    private router = router;

    keys = ko.observableArray<configurationKey>();
    selectedKey = ko.observable<configurationKey>().subscribeTo("ActivateConfigurationKey").distinctUntilChanged();
    selectedKeyValue = ko.observable<Pair<string, string>>();
    currentKey = ko.observable<configurationKey>();
    configurationEditor: AceAjax.Editor;
    configurationKeyText = ko.observable<string>();
    isBusy = ko.observable(false);
    isSaveEnabled: KnockoutComputed<boolean>;
    subscription: any;

    constructor() {
        super();
        this.selectedKey.subscribe(k => this.selectedKeyChanged(k));

        // When we programmatically change a configuration doc, push it into the editor.
        this.subscription = this.configurationKeyText.subscribe(() => this.updateConfigurationText());
    }

    canActivate(args: any) {
        return true;
    }

    activate(navigationArgs) {
        super.activate(navigationArgs);

        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.configurationKeyText]);

        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    attached() {
        this.activeFilesystem.subscribe(x => {
            this.loadKeys(x);
        });

        (<any>$('.keys-collection')).contextmenu({
            target: '#keys-context-menu'
        });

        this.loadKeys(this.activeFilesystem());
        this.initializeDocEditor();
        this.configurationEditor.focus();
    }

    initializeDocEditor() {
        // Startup the Ace editor with JSON syntax highlighting.
        this.configurationEditor = ace.edit("configurationEditor");
        this.configurationEditor.setTheme("ace/theme/github");
        this.configurationEditor.setFontSize("16px");
        this.configurationEditor.getSession().setMode("ace/mode/json");
        $("#configurationEditor").on('keyup', ".ace_text-input", () => this.storeEditorTextIntoObservable());
        this.updateConfigurationText();
    }

    storeEditorTextIntoObservable() {
        if (this.configurationEditor) {
            var text = this.configurationEditor.getSession().getValue();

            this.subscription.dispose();
            this.configurationKeyText(text);
            this.subscription = this.configurationKeyText.subscribe(() => this.updateConfigurationText());
        }
    }

    updateConfigurationText() {
        if (this.configurationEditor) {
            this.configurationEditor.getSession().setValue(this.configurationKeyText());
        }
    }

    loadKeys(fs: filesystem){
        new getConfigurationCommand(fs)
            .execute()
            .done( (x: configurationKey[]) => {
                this.keys(x);
                if (x.length > 0) {
                    this.selectedKey(x[0]);
                }
            });
    }

    selectKey(key: configurationKey) {
        key.activate();
        router.navigate(appUrl.forFilesystemConfigurationWithKey(this.activeFilesystem(), key.key));
    }

    selectedKeyChanged(selected: configurationKey) {
        if (selected) {
            this.isBusy(true);
            selected.getValues().done(data => {
                this.configurationKeyText(data);
            }).always(() => {
                this.isBusy(false);
            });

            this.currentKey(selected);
        }
    }

    selectKeyValue(selection: Pair<string, string>) {
        this.selectedKeyValue(selection);
    }

    save() {
        var jsonConfigDoc = JSON.parse(this.configurationKeyText());
        var saveCommand = new saveConfigurationCommand(this.activeFilesystem(), this.currentKey(), jsonConfigDoc);
        var saveTask = saveCommand.execute();
        saveTask.done((result) => {
            // Resync Changes
            viewModelBase.dirtyFlag().reset();
        });
    }

    refreshConfig() {
        //this.selectKey(this.currentKey());
    }

    deleteKey() {
        throw new Error("Not Implemented");
    }
} 

export = configuration;