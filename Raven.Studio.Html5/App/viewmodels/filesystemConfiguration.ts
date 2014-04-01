import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import ace = require("ace/ace");

import pagedList = require("common/pagedList");
import filesystem = require("models/filesystem");
import collection = require("models/filesystemConfigurationKeyCollection");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import getFilesystemConfigurationCommand = require("commands/getFilesystemConfigurationCommand");
import getFilesystemConfigurationByKeyCommand = require("commands/getFilesystemConfigurationByKeyCommand");

class filesystemConfiguration extends viewModelBase {

    private router = router;

    docEditor: AceAjax.Editor;

    currentKey = ko.observable<string>();
    keys = ko.observableArray<string>();

    static containerId = "#settingsContainer";

    constructor() {
        super();
    }

    attached() {
       
        this.createKeyboardShortcut("F2", () => this.navigateToEditSettingsDocument(), filesystemConfiguration.containerId);       
        this.loadKeys(this.activeFilesystem());        
    }

    activate(args) {
        super.activate(args); 
        if (args.key != null) {
            this.currentKey(args.key);
        }  

        this.initializeDbDocEditor();
        this.currentKey.subscribe(x => this.loadEditorWithKey(x)); 
    }

    private initializeDbDocEditor() {
        // Startup the read-only Ace editor with JSON syntax highlighting.
        this.docEditor = ace.edit("dbDocEditor");
        this.docEditor.setTheme("ace/theme/github");
        this.docEditor.setFontSize("16px");
        this.docEditor.getSession().setMode("ace/mode/json");        
        this.docEditor.setReadOnly(true);       
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

    loadEditorWithKey(key: string) { 
        if (key != null) {
            new getFilesystemConfigurationByKeyCommand(this.activeFilesystem(), key)
                .execute()
                .done(x => this.handleDocument(x));
        }               
    }

    private handleDocument(doc: any) {
        var docSettings = this.stringify(doc);
        this.docEditor.getSession().setValue(docSettings);
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

export = filesystemConfiguration;