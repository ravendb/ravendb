import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import ace = require("ace/ace");
import shell = require("viewmodels/shell");

import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import getConfigurationCommand = require("commands/filesystem/getConfigurationCommand");
import saveConfigurationCommand = require("commands/filesystem/saveConfigurationCommand");
import configurationKey = require("models/filesystem/configurationKey");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import deleteConfigurationKeys = require("viewmodels/filesystem/deleteConfigurationKeys");
import messagePublisher = require("common/messagePublisher");
import Pair = require("common/pair");

class configuration extends viewModelBase {

    static configSelector = "#settingsContainer";
    private router = router;

    appUrls: computedAppUrls;

    keys = ko.observableArray<configurationKey>();
    text: KnockoutComputed<string>;
    selectedKeyValue = ko.observable<Pair<string, string>>();
    selectedKey = ko.observable<configurationKey>().subscribeTo("ActivateConfigurationKey").distinctUntilChanged();
    currentKey = ko.observable<configurationKey>();
    configurationEditor: AceAjax.Editor;
    configurationKeyText = ko.observable<string>();
    isBusy = ko.observable(false);
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedConfiguration = ko.observable('');
    subscription: any;
    enabled: boolean = true;

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

        this.appUrls = appUrl.forCurrentFilesystem();

        if (!this.subscription) {
            this.subscription = shell.currentResourceChangesApi()
                .watchFsConfig((e: filesystemConfigNotification) => {
                    switch (e.Action) {
                        case filesystemConfigurationChangeAction.Set:
                            this.addKey(e.Name);
                            break;
                        case filesystemConfigurationChangeAction.Delete:
                            this.removeKey(e.Name);
                            break;
                        default:
                            console.error("Unknown notification action.");

                    }
                });
        }

        if (this.currentKey()) {
            this.loadedConfiguration(this.currentKey().key);
        }

        this.dirtyFlag = new ko.DirtyFlag([this.configurationKeyText]);

        this.isSaveEnabled = ko.computed(() => {
            return this.dirtyFlag().isDirty();
        });
    }

    attached() {
        this.activeFilesystem.subscribe(x => {
            this.loadKeys(x);
        });

        (<any>$('.keys-collection')).contextmenu({
            target: '#keys-context-menu'
        });

        this.initializeDocEditor();
        this.loadKeys(this.activeFilesystem());
        this.configurationEditor.focus();
        this.setupKeyboardShortcuts();
    }

    selectKeyValue(selection: Pair<string, string>) {
        this.selectedKeyValue(selection);
    }

    setupKeyboardShortcuts() {
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteConfiguration(), configuration.configSelector);
    }

    initializeDocEditor() {
        // Startup the Ace editor with JSON syntax highlighting.
        // TODO: Just use the simple binding handler instead.
        this.configurationEditor = ace.edit("configurationEditor");
        this.configurationEditor.setTheme("ace/theme/xcode");
        this.configurationEditor.setFontSize("16px");
        this.configurationEditor.getSession().setMode("ace/mode/json");
    }

    updateConfigurationText() {
        if (this.configurationEditor) {
            this.configurationEditor.getSession().setValue(this.configurationKeyText());
        }
    }

    loadKeys(fs: filesystem) {
        if (this.enabled) {
            new getConfigurationCommand(fs)
                .execute()
                .done( (x: configurationKey[]) => {
                    this.keys(x);
                    if (x.length > 0) {
                        this.selectKey(x[0]);
                    }
                    else {
                        this.enableEditor(false);
                    }
                });
        }
    }

    selectKey(key: configurationKey) {
        key.activate();
    }

    enableEditor(enable: boolean) {
        this.configurationEditor.setReadOnly(!enable);
        this.configurationEditor.getSession().setUseWorker(enable);
        if (!enable) {
            this.configurationKeyText("");
            this.dirtyFlag().reset();
        }

        this.enabled = enable;
    }

    selectedKeyChanged(selected: configurationKey) {
        if (selected) {
            this.isBusy(true);
            selected.getValues().done(data => {
                this.configurationKeyText(data);
                this.loadedConfiguration(this.selectedKey().key);
            }).always(() => {
                this.dirtyFlag().reset();
                this.isBusy(false);
            });

            this.currentKey(selected);
        }
    }

    save() {
        var jsonConfigDoc = JSON.parse(this.configurationKeyText());
        var saveCommand = new saveConfigurationCommand(this.activeFilesystem(), this.currentKey(), jsonConfigDoc);
        var saveTask = saveCommand.execute();
        saveTask.done((result) => {
            this.dirtyFlag().reset(); // Resync Changes
        });
    }

    refreshConfig() {
        this.selectedKeyChanged(this.currentKey());
    }

    deleteConfiguration() {
        require(["viewmodels/filesystem/deleteConfigurationKeys"], deleteConfigurationKey => {
            var deleteConfigurationKeyViewModel: deleteConfigurationKeys = new deleteConfigurationKeys(this.activeFilesystem(), [this.currentKey()]);
            deleteConfigurationKeyViewModel
                .deletionTask
                .done(() => {
                    this.removeKey(this.currentKey().key);
                });
            app.showDialog(deleteConfigurationKeyViewModel);
        });
    }

    removeKey(key: string) {
        var foundKey = this.keys().filter((x: configurationKey) => { return (x.key == key) });

        if (foundKey.length > 0) {
            var currentIndex = this.keys.indexOf(this.currentKey());
            var foundIndex = this.keys.indexOf(foundKey[0]);
            var newIndex = currentIndex;
            if (currentIndex + 1 == this.keys().length) {
                newIndex = currentIndex - 1;
            }

            this.keys.remove(foundKey[0]);
            if (this.keys()[newIndex] && currentIndex == foundIndex) {
                this.selectKey(this.keys()[newIndex]);
            }
            else {
                this.enableEditor(false);
            }
        }
    }

    addKey(key: string) {
        var foundKey = this.keys().filter((x: configurationKey) => { return (x.key == key) });
        if (foundKey.length <= 0) {
            var newKey = new configurationKey(this.activeFilesystem(), key);
            this.keys.push(newKey);
            if (this.keys().length > 0 && !this.enabled) {
                this.enableEditor(true);
            }
            this.selectKey(newKey);
        }
    }

    newConfigurationKey() {
        require(["viewmodels/filesystem/createConfigurationKey"], createConfigurationKey => {
            var createConfigurationKeyViewModel = new createConfigurationKey(this.keys());
            createConfigurationKeyViewModel
                .creationTask
                .done((key: string) => {
                    new saveConfigurationCommand(this.activeFilesystem(), new configurationKey(this.activeFilesystem(), key), JSON.parse("{}")).execute()
                        .done(() => this.addKey(key))
                        .fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not create Configuration Key!", errorThrown));
                });
            app.showDialog(createConfigurationKeyViewModel);
        });
    }

} 

export = configuration;