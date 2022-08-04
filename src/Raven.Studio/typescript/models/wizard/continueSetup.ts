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
    
    isZipSecure: KnockoutComputed<boolean>;
    isZipValid: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup;

    static readonly securePrefix = "https://";
    static readonly unsecurePrefix = "http://";
    
    constructor() {
        _.bindAll(this, "fileSelected", "onConfigEntrySelected");

        this.isZipSecure = ko.pureComputed(() => {
            return this.areNodesSecure();
        })
        
        this.isZipValid = ko.pureComputed(() => {
            return this.areNodesValid();
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
        
        this.isZipValid.extend({
            validation: [
                {
                    validator: (val: boolean) => val,
                    message: "Invalid nodes configuration in zip file"
                }
            ]
        })
        
        this.validationGroup = ko.validatedObservable({
            importedFileName: this.importedFileName,
            nodeTag: this.nodeTag,
            isZipValid: this.isZipValid
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
                const nodes = nodesInfo.map(x => {
                    return { tag: x.Tag, serverUrl: x.ServerUrl, publicServerUrl: x.PublicServerUrl }
                });

                this.nodesInfo(nodes);
            });
    }

    private areNodesSecure(): boolean {
        const nodes = this.nodesInfo();

        if (!this.zipFile() || !nodes.length) {
            return false;
        }

        let secure = true;
        nodes.forEach(node => {
            if (!this.isNodeSecure(node)) {
                secure = false;
            }
        });

        return secure;
    }

    private isNodeSecure(node: configurationNodeInfo): boolean {
        return node.publicServerUrl && node.publicServerUrl.startsWith(continueSetup.securePrefix);
    }
    
    private areNodesValid(): boolean {
        let isValid = true;
        
        const nodes = this.nodesInfo();
        if (!nodes.length) {
            return true;
        }
        
        const firstNode = nodes[0];
        
        
        if (firstNode.publicServerUrl) {
            nodes.forEach(x => {
                if (!x.publicServerUrl || !x.publicServerUrl.startsWith(continueSetup.securePrefix)) {
                    console.log('false');
                    isValid = false;
                }
            })
        } else {
            nodes.forEach(x => {
                if (!x.serverUrl || !x.serverUrl.startsWith(continueSetup.unsecurePrefix)) {
                    console.log('false');
                    isValid = false;
                }
            })
        }
        
        return isValid;
    }
}

export = continueSetup;
