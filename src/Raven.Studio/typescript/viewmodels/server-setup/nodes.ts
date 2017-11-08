import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import nodeInfo = require("models/setup/nodeInfo");

import serverSetup = require("models/setup/serverSetup");

class nodes extends setupStep {

    editedNode = ko.observable<nodeInfo>();
    
    provideCertificates = ko.pureComputed(() => {
        const mode = this.model.mode();
        return mode && mode === "Secured";
    });
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("removeNode", "editNode");
    }

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && (mode === "Secured" || mode === "LetsEncrypt")) {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        if (this.model.nodes().length) {
            this.editedNode(this.model.nodes()[0]);
        }
    }
    
    save() {
        const nodes = this.model.nodes();
        let isValid = true;
        
        nodes.forEach(node => {
            if (!this.isValid(node.validationGroup)) {
                isValid = false;
            }
            
            node.ips().forEach(entry => {
                if (!this.isValid(entry.validationGroup)) {
                    isValid = false;
                }
            });
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
                router.navigate("#agreement");
                break;
            case "Secured":
                router.navigate("#certificate");
                break;
            default:
                router.navigate("#welcome");
        }
    }
  
    addNode() {
        const node = new nodeInfo();
        this.model.nodes.push(node);
        this.editedNode(node);
        this.updateNodeTags();
    }

    editNode(node: nodeInfo) {
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
           idx++;
        });
    }

}

export = nodes;
