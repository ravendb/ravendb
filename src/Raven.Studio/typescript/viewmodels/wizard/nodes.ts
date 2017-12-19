import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import nodeInfo = require("models/wizard/nodeInfo");

import serverSetup = require("models/wizard/serverSetup");

class nodes extends setupStep {

    currentStep: number;

    confirmation = ko.observable<boolean>(false);
    confirmationValidationGroup = ko.validatedObservable({
        confirmation: this.confirmation
    });
    
    editedNode = ko.observable<nodeInfo>();
        
    defineServerUrl: KnockoutComputed<boolean>;
    showDnsInfo: KnockoutComputed<boolean>;
    provideCertificates: KnockoutComputed<boolean>;
    showAgreement: KnockoutComputed<boolean>;
    showFullDomain: KnockoutComputed<boolean>;
    showAdvancedSettings: KnockoutComputed<boolean>;
   
    maxNodesAddedMsg: KnockoutComputed<string>;
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("removeNode", "editNode");

        this.confirmation.extend({
            validation: [
                {
                    validator: (val: boolean) => val === true,
                    message: "You must accept Let's Encrypt Subscriber Agreement"
                }
            ]
        });
        
        this.defineServerUrl = ko.pureComputed(() => {
            return this.model.mode() === "Secured" && !this.model.certificate().wildcardCertificate();
        });
        
        this.provideCertificates = ko.pureComputed(() => {
            const mode = this.model.mode();
            return mode && mode === "Secured";
        });

        this.showDnsInfo = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.showFullDomain = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.showAgreement = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.showAdvancedSettings = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
       
        this.maxNodesAddedMsg = ko.pureComputed(() => {   
            const numberOfNodesAdded = this.model.nodes().length;
            const maxNodesAllowed = this.model.license().maxClusterSize();
            
            // Limit number of nodes that can be added according to license, if in 'LetsEncrypt' flow
            if (this.model.mode() === 'LetsEncrypt') {
                
                if (numberOfNodesAdded === maxNodesAllowed) {
                    return `Only ${maxNodesAllowed} nodes are allowed with your current ${this.model.license().licenseType()} license edition`;
                }
            }
            
            return null;
        });
    }

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && (mode === "Secured" || mode === "LetsEncrypt")) {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    activate(args: any) {
        super.activate(args);
        
        this.updateNodeTags();
        
        switch (this.model.mode()) {
            case "LetsEncrypt":
                this.currentStep = 4;
                break;
            case "Secured":
                this.currentStep = 3;
                break;
        }
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        if (this.model.nodes().length) {
            this.editedNode(this.model.nodes()[0]);
        }

        this.setupDisableReasons();
    }
    
    save() {
        const nodes = this.model.nodes();
        let isValid = true;
        
        if (this.showAgreement()) {
            if (!this.isValid(this.confirmationValidationGroup)) {
                isValid = false;
            }
        }
        
        let focusedOnInvalidNode = false;
        
        nodes.forEach(node => {
            let validNodeConfig = true;
            
            if (!this.isValid(node.validationGroup)) {
                validNodeConfig = false;
            }
            
            node.ips().forEach(entry => {
                if (!this.isValid(entry.validationGroup)) {
                    validNodeConfig = false;
                }
            });
            
            if (!validNodeConfig) {
                isValid = false;
                if (!focusedOnInvalidNode) {
                    this.editedNode(node);
                    focusedOnInvalidNode = true;
                }
            }
        });
        
        if (!this.isValid(this.model.nodesValidationGroup)) {
            isValid = false;
        }
        
        if (isValid) {
            router.navigate("#finish");
        }
    }

    back() {
        switch (this.model.mode()) {
            case "LetsEncrypt":
                router.navigate("#domain");
                break;
            case "Secured":
                router.navigate("#certificate");
                break;
            default:
                router.navigate("#welcome");
        }
    }
  
    addNode() {
        const node = new nodeInfo(this.model.hostnameIsNotRequired);
        this.model.nodes.push(node);
        this.editedNode(node);
        this.updateNodeTags();
    }

    editNode(node: nodeInfo) {
        node.showAdvancedSettings(!!node.externalIpAddress());
        this.editedNode(node);
    }
    
    removeNode(node: nodeInfo) {
        this.model.nodes.remove(node);
        if (this.editedNode() === node) {
            this.editedNode(null);
        }
        
        this.updateNodeTags();
    }
    
    updateNodeTags() {
        let idx = 0;
        this.model.nodes().forEach(node => {
           node.nodeTag(serverSetup.nodesTags[idx]);
           
           if (idx === 0 && this.model.fixPortNumberOnLocalNode()) {
               node.port(this.model.fixedLocalPort().toString());
           }
           
           idx++;
        });
    }    
}

export = nodes;
