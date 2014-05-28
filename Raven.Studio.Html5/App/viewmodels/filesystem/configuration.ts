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
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import createConfigurationKey = require("viewmodels/filesystem/createConfigurationKey");
import deleteConfigurationKeys = require("viewmodels/filesystem/deleteConfigurationKeys");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");

class configuration extends viewModelBase {

    static configSelector = "#settingsContainer";
    private router = router;

    keys = ko.observableArray<configurationKey>();
    text: KnockoutComputed<string>;
    selectedKey = ko.observable<configurationKey>().subscribeTo("ActivateConfigurationKey").distinctUntilChanged();
    selectedKeyValue = ko.observable<Pair<string, string>>();
    currentKey = ko.observable<configurationKey>();
    configurationEditor: AceAjax.Editor;
    configurationKeyText = ko.observable<string>();
    isBusy = ko.observable(false);
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedConfiguration = ko.observable('');
    subscription: any;

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.selectedKey.subscribe(k => this.selectedKeyChanged(k));

        // When we programmatically change a configuration doc, push it into the editor.
        //this.subscription = this.configurationKeyText.subscribe(() => this.updateConfigurationText());

        this.text = ko.computed({
            read: () => {
                return this.configurationKeyText();
            },
            write: (text: string) => {
                this.configurationKeyText(text);
            },
            owner: this
        });
    }

    canActivate(args: any) {
        return true;
    }

    activate(navigationArgs) {
        super.activate(navigationArgs);
        if (this.currentKey()) {
            this.loadedConfiguration(this.currentKey().key);
        }

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
        this.setupKeyboardShortcuts();
    }

    setupKeyboardShortcuts() {
        //this.createKeyboardShortcut("alt+shift+d", () => this.currentKey(), editDocument.editDocSelector);
        //this.createKeyboardShortcut("alt+shift+m", () => this.focusOnMetadata(), editDocument.editDocSelector);
        //this.createKeyboardShortcut("alt+c", () => this.focusOnEditor(), editDocument.editDocSelector);
        //this.createKeyboardShortcut("alt+home", () => this.firstDocument(), editDocument.editDocSelector);
        //this.createKeyboardShortcut("alt+end", () => this.lastDocument(), editDocument.editDocSelector);
        //this.createKeyboardShortcut("alt+page-up", () => this.previousDocumentOrLast(), editDocument.editDocSelector);
        //this.createKeyboardShortcut("alt+page-down", () => this.nextDocumentOrFirst(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteConfiguration(), configuration.configSelector);
    }

    initializeDocEditor() {
        // Startup the Ace editor with JSON syntax highlighting.
        this.configurationEditor = ace.edit("configurationEditor");
        this.configurationEditor.setTheme("ace/theme/github");
        this.configurationEditor.setFontSize("16px");
        this.configurationEditor.getSession().setMode("ace/mode/json");
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
                    this.selectKey(x[0]);
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
                this.loadedConfiguration(this.selectedKey().key);
            }).always(() => {
                viewModelBase.dirtyFlag().reset();
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
        this.selectKey(this.currentKey());
    }

    deleteConfiguration() {
        require(["viewmodels/filesystem/deleteConfigurationKeys"], deleteConfigurationKey => {
            var deleteConfigurationKeyViewModel: deleteConfigurationKeys = new deleteConfigurationKeys(this.activeFilesystem(), [this.currentKey()]);
            deleteConfigurationKeyViewModel
                .deletionTask
                .done(() => {
                    var currentIndex = this.keys.indexOf(this.currentKey());
                    var newIndex = currentIndex;
                    if (currentIndex + 1 == this.keys().length) {
                        newIndex = currentIndex - 1;
                    }

                    this.keys.remove(this.currentKey());
                    if (this.keys()[newIndex]) {
                        this.selectKey(this.keys()[newIndex]);
                    }
                });
            app.showDialog(deleteConfigurationKeyViewModel);
        });
    }

    newConfigurationKey() {
        require(["viewmodels/filesystem/createConfigurationKey"], createFilesystem => {
            var createConfigurationKeyViewModel: createConfigurationKey = new createConfigurationKey(this.keys());
            createConfigurationKeyViewModel
                .creationTask
                .done((key: string) => {
                    var newKey = new configurationKey(this.activeFilesystem(), key);
                    new saveConfigurationCommand(this.activeFilesystem(), newKey, JSON.parse("{}")).execute()
                        .done(() => {
                            this.keys.push(newKey);
                            this.selectKey(newKey);
                        })
                        .fail((qXHR, textStatus, errorThrown) => {
                            ko.postbox.publish("Alert", new alertArgs(alertType.danger, "Could not create Configuration Key.", errorThrown));
                        });
                });
            app.showDialog(createConfigurationKeyViewModel);
        });
    }

    modelPolling() {
    }
} 

export = configuration;