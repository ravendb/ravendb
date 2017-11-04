import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import nodeInfo = require("models/setup/nodeInfo");

class nodes extends setupStep {

    provideCertificates = ko.pureComputed(() => {
        const mode = this.model.mode();
        return mode && mode === "Secured";
    });
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("removeNode");
    }

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && (mode === "Secured" || mode === "LetsEncrypt")) {
            return $.when({ can: true });
        }

        return $.when({redirect: "#welcome" });
    }
    
    //TODO: remember to update 
    
    save() {
        //TODO: validate - at least one node
        
        const nodes = this.model.nodes();
        let isValid = true;
        
        nodes.forEach(node => {
            if (!this.isValid(node.validationGroup)) {
                isValid = false;
            }
        });
        
        if (!this.isValid(this.model.nodesValidationGroup)) {
            isValid = false;
        }
        
        if (isValid) {
            alert("all ok");
            
            //TODO: save data
        }
    }
    
    addNode() {
        this.model.nodes.push(nodeInfo.empty(this.model.useOwnCertificates));
    }

    removeNode(node: nodeInfo) {
        this.model.nodes.remove(node);
    }

}

export = nodes;
