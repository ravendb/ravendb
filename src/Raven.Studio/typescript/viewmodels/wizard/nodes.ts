import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import nodeInfo = require("models/wizard/nodeInfo");
import serverSetup = require("models/wizard/serverSetup");
import popoverUtils = require("common/popoverUtils");
import ipEntry = require("models/wizard/ipEntry");

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
    canCustomizeIp: KnockoutComputed<boolean>;
   
    maxNodesAddedMsg: KnockoutComputed<string>;
    showNodeTagInUrl: KnockoutComputed<boolean>;
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("removeNode", "editNode", "addIpAddressFromNode", "removeIpFromNode");

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
        this.canCustomizeIp = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
       
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
        
        this.showNodeTagInUrl = ko.pureComputed(() => this.model.mode() !== "Secured" || this.model.certificate().wildcardCertificate());
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

            let firstNode = this.model.nodes()[0];

            if (firstNode.ips().length === 0 ) {
                firstNode.ips.push(new ipEntry());
            }

            this.editedNode(firstNode);

            this.initTooltips();
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
        this.initTooltips();
    }

    editNode(node: nodeInfo) {
        this.editedNode(node);
        this.initTooltips();
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
           
           if (idx === 0 && this.model.fixTcpPortNumberOnLocalNode()) {
               node.tcpPort(this.model.fixedTcpPort().toString());
           }
           
           idx++;
        });
    }    

    private initTooltips() {
        const ownCerts = this.model.mode() === "Secured";
        
        popoverUtils.longWithHover($("#dns-name-info"),
            {
                content:
                "Domain name that will be used to reach the server on this node.<br />" +
                "Note: It <strong>must</strong> be associated with the chosen IP Address below.",
                placement: "top"
            });

        const ipText = ownCerts ? "IP Address or Hostname that should already be associated with DNS name in certificate. " : "IP Address or Hostname that will be associated with the DNS Name.";
        
        const ipAddressInfo =  ipText + "<br/>" +
            "For example:<br/>" +
            "<ul>" +
            "  <li>10.0.0.84</li>" +
            "  <li>127.0.0.1</li>" +
            "  <li>localhost</li>" +
            "  <li>john-pc</li>" +
            "</ul>";

        popoverUtils.longWithHover($("#ip-address-info"),
            {
                content: ipAddressInfo,
                placement: "top"
            });

        popoverUtils.longWithHover($("#ip-address-info-with-warning"),
            {
                //TODO: check if we should display this in own cert flow 
                content: ipAddressInfo + "<strong>Note:</strong> If Hostname is used then an external ip must also be provided.",
                placement: "top"
            });
    }

    addIpAddressFromNode(node: nodeInfo) {
        node.addIpAddress();
        this.initTooltips();
    }

    removeIpFromNode(node: nodeInfo, ipEntry: ipEntry) {
        node.removeIp(ipEntry);
        this.initTooltips();
    }
}

export = nodes;
