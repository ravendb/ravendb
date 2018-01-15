/// <reference path="../../../typings/tsd.d.ts"/>

import extractNodesInfoFromPackageCommand = require("commands/wizard/extractNodesInfoFromPackageCommand");

class continueSetup {

    importedFileName = ko.observable<string>();
    hasFileSelected = ko.observable(false);
    nodesInfo = ko.observableArray<Raven.Server.Web.System.ConfigurationNodeInfo>([]);
    
    
    zipFile = ko.observable<string>();
    nodeTag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        _.bindAll(this, "fileSelected", "onConfigEntrySelected");
        
        this.initValidation();
    }
    
    private initValidation() {
        this.importedFileName.extend({
            required: true
        });
        this.nodeTag.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            importedFileName: this.importedFileName,
            nodeTag: this.nodeTag
        });
    }
    
    
    onConfigEntrySelected(item: Raven.Server.Web.System.ConfigurationNodeInfo) {
        this.nodeTag(item.Tag);
        this.serverUrl(item.PublicServerUrl);
    }

    fileSelected(fileInput: HTMLInputElement) {
        if (fileInput.files.length === 0) {
            return;
        }

        const fileName = fileInput.value;
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.hasFileSelected(isFileSelected);
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
        
        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = () => {
            const dataUrl = reader.result;
            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.zipFile(dataUrl.substr(dataUrl.indexOf(",") + 1));
            
            this.fetchNodesInfo();
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsDataURL(file);
    }
    
    private fetchNodesInfo() {
        new extractNodesInfoFromPackageCommand(this.zipFile())
            .execute()
            .done((nodesInfo) => {
                this.nodesInfo(nodesInfo);
            });
    }
}

export = continueSetup;
