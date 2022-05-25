/// <reference path="../../../typings/tsd.d.ts"/>

import extractNodesInfoFromPackageCommand = require("commands/wizard/extractNodesInfoFromPackageCommand");
import fileImporter = require("common/fileImporter");

interface configurationNodeInfo  {
    publicServerUrl: string;
    serverUrl: string;
    tag: string;
}

class continueSetup {

    importedFileName = ko.observable<string>();
    hasFileSelected = ko.observable(false);
    nodesInfo = ko.observableArray<configurationNodeInfo>([]);
    
    zipFile = ko.observable<string>();
    nodeTag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    
    isZipFileSecure: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        _.bindAll(this, "fileSelected", "onConfigEntrySelected");
        
        this.isZipFileSecure = ko.pureComputed(() => {
            const nodes = this.nodesInfo();
            return this.zipFile() && nodes.length && !!nodes[0].publicServerUrl; 
            // nodes from unsecure zip contain ServerUrl
            // nodes from secure zip contain PublicServerUrl
        })
        
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
    
    onConfigEntrySelected(item: configurationNodeInfo) {
        this.nodeTag(item.tag);
        this.serverUrl(item.publicServerUrl || item.serverUrl);
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsDataURL(fileInput, (dataUrl, fileName) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;
            this.hasFileSelected(isFileSelected);
            this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);

            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.zipFile(dataUrl.substr(dataUrl.indexOf(",") + 1));

            this.fetchNodesInfo();
        });
    }
    
    private fetchNodesInfo() {
        new extractNodesInfoFromPackageCommand(this.zipFile())
            .execute()
            .done((nodesInfo) => {
                let nodes = nodesInfo.map(x => {
                    return { tag: x.Tag, serverUrl: x.ServerUrl, publicServerUrl: x.PublicServerUrl }
                });

                this.nodesInfo(nodes);
            });
    }
}

export = continueSetup;
