import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import nodeInfo = require("models/setup/nodeInfo");

import serverSetup = require("models/setup/serverSetup");

//TODO: validate certificate password

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
    
    save() {
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
            router.navigate("#finish");
        }
    }
    
    back() {
        switch (this.model.mode()) {
            case "LetsEncrypt":
                router.navigate("#agreement");
                break;
            case "Secured":
                router.navigate("#license");
                break;
        }
    }
  
    addNode() {
        this.model.nodes.push(nodeInfo.empty(this.model.useOwnCertificates));
    }

    removeNode(node: nodeInfo) {
        this.model.nodes.remove(node);
    }

    getLabelFor(idx: number) {
        
        const fullUrl =  this.model.mode() === "LetsEncrypt";
        const tag = serverSetup.nodesTags[idx];
        if (fullUrl) {
            return tag + "." + this.model.domain().domain() + ".dbs.local.ravendb.net";
        } else {
            return tag;
        }
    }
}

export = nodes;
