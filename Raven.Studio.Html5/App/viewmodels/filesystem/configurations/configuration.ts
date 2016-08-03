/// <reference path="../../../../Scripts/typings/ace/ace.amd.d.ts" />

import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import changesContext = require("common/changesContext");
import ace = require("ace/ace");
import viewModelBase = require("viewmodels/viewModelBase");
import getConfigurationCommand = require("commands/filesystem/getConfigurationCommand");
import filesystem = require("models/filesystem/filesystem");
import configurationKey = require("models/filesystem/configurationKey");
import changeSubscription = require('common/changeSubscription');
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import Pair = require("common/pair");
import saveConfigurationCommand = require("commands/filesystem/saveConfigurationCommand");
import deleteConfigurationKeys = require("viewmodels/filesystem/configurations/deleteConfigurationKeys");
import createConfigurationKey = require("viewmodels/filesystem/configurations/createConfigurationKey");

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
    configurationKeyText = ko.observable<string>('').extend({ required: true });
    isBusy = ko.observable(false);
    isSaveEnabled: KnockoutComputed<boolean>;
    editor: AceAjax.Editor;
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

    activate(navigationArgs) {
        super.activate(navigationArgs);

        this.updateHelpLink("J3KIEN");

        this.appUrls = appUrl.forCurrentFilesystem();

        this.dirtyFlag = new ko.DirtyFlag([this.configurationKeyText]);

        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
        return this.loadKeys(this.activeFilesystem());
    }

    attached() {
        super.attached();
        this.activeFilesystem.subscribe(x => {
            this.loadKeys(x);
        });

        (<any>$('.keys-collection')).contextmenu({
            target: '#keys-context-menu'
        });

        this.initializeDocEditor();
        this.configurationEditor.focus();
        this.setupKeyboardShortcuts();
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $("#configurationEditor");
        if (editorElement.length > 0) {
            this.editor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }
        $("#configurationEditor").on('DynamicHeightSet', () => this.editor.resize());

        this.focusOnEditor();
    }

    detached() {
        super.detached();
        this.selectedKey.unsubscribeFrom("ActivateConfigurationKey");
        $("#configurationEditor").off('DynamicHeightSet');

    }

    private focusOnEditor() {
        if (!!this.editor) {
            this.editor.focus();
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [changesContext.currentResourceChangesApi().watchFsConfig((e: filesystemConfigNotification) => this.processFsConfigNotification(e)) ];
    }

    processFsConfigNotification(e: filesystemConfigNotification) {
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
            return new getConfigurationCommand(fs)
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
        return $.Deferred().resolve();
    }

    selectKey(key: configurationKey) {
        key.activate();
    }

    enableEditor(enable: boolean) {

        if (this.configurationEditor) {

            this.configurationEditor.setReadOnly(!enable);
            this.configurationEditor.getSession().setUseWorker(enable);

            if (!enable) {
                this.configurationKeyText("");
                this.dirtyFlag().reset();
            }
        }

        this.enabled = enable;
    }

    selectedKeyChanged(selected: configurationKey) {
        if (selected) {
            this.isBusy(true);
            selected.getValues().done(data => {
                this.configurationKeyText(data);
            }).always(() => {
                this.dirtyFlag().reset();
                this.isBusy(false);
            });

            this.currentKey(selected);
            this.focusOnEditor();
        }
    }

    save() {
        var message = "";
        try {
            var jsonConfigDoc = JSON.parse(this.configurationKeyText());
        } catch (e) {
            if (jsonConfigDoc == undefined) {
                message = "The configuration key data isn't a legal JSON expression!";
            }
            this.focusOnEditor();
        }
        if (message) {
            messagePublisher.reportError(message, undefined, undefined, false);
            return;
        }

        var saveCommand = new saveConfigurationCommand(this.activeFilesystem(), this.currentKey(), jsonConfigDoc);
        var saveTask = saveCommand.execute();
        saveTask.done(() => this.dirtyFlag().reset());
    }

    refreshConfig() {
        this.selectedKeyChanged(this.currentKey());
    }

    deleteConfiguration() {
        var deleteConfigurationKeyViewModel = new deleteConfigurationKeys(this.activeFilesystem(), [this.currentKey()]);
        deleteConfigurationKeyViewModel
            .deletionTask
            .done(() => {
                this.removeKey(this.currentKey().key);
            });
        app.showDialog(deleteConfigurationKeyViewModel);
    }

    removeKey(key: string) {
        var foundKey = this.keys().filter((configKey: configurationKey) => configKey.key === key );

        if (foundKey.length > 0) {
            var currentIndex = this.keys.indexOf(this.currentKey());
            var foundIndex = this.keys.indexOf(foundKey[0]);
            var newIndex = currentIndex;
            if (currentIndex + 1 === this.keys().length) {
                newIndex = currentIndex - 1;
            }

            this.keys.remove(foundKey[0]);
            if (this.keys()[newIndex] && currentIndex === foundIndex) {
                this.selectKey(this.keys()[newIndex]);
            }
            else {
                this.enableEditor(false);
            }
        }
    }

    addKey(key: string) {
        var foundKey = this.keys.first((configKey: configurationKey) => configKey.key === key);

        if (!foundKey) {
            var newKey = new configurationKey(this.activeFilesystem(), key);
            this.keys.push(newKey);
            if (this.keys().length > 0 && !this.enabled) {
                this.enableEditor(true);
            }
            return newKey;
        }

        return foundKey;
    }

    newConfigurationKey() {
        var createConfigurationKeyViewModel = new createConfigurationKey(this.keys());
        createConfigurationKeyViewModel
            .creationTask
            .done((key: string) => {
                new saveConfigurationCommand(this.activeFilesystem(), new configurationKey(this.activeFilesystem(), key), JSON.parse("{}")).execute()
                    .done(() => {
                        var newKey = this.addKey(key);
                        this.selectKey(newKey);
                    })
                    .fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not create Configuration Key!", errorThrown));
            });
        app.showDialog(createConfigurationKeyViewModel);
    }
} 

export = configuration;
