import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import nodeInfo = require("models/wizard/nodeInfo");
import serverSetup = require("models/wizard/serverSetup");
import popoverUtils = require("common/popoverUtils");
import ipEntry = require("models/wizard/ipEntry");
import databaseStudioConfigurationModel = require("models/database/settings/databaseStudioConfigurationModel");

class nodes extends setupStep {

    static environments = databaseStudioConfigurationModel.environments;
    
    currentStep: number;
    
    remoteNodeIpOptions = ko.observableArray<string>(['0.0.0.0']);

    confirmation = ko.observable<boolean>(false);
    confirmationValidationGroup = ko.validatedObservable({
        confirmation: this.confirmation
    });
    
    editedNode = ko.observable<nodeInfo>();
        
    defineServerUrl: KnockoutComputed<boolean>;
    showDnsInfo: KnockoutComputed<boolean>;
    showAgreement: KnockoutComputed<boolean>;
    requirePublicIpWhenBindAllUsed: KnockoutComputed<boolean>;
    showFullDomain: KnockoutComputed<boolean>;
    canCustomizeExternalIpsAndPorts: KnockoutComputed<boolean>;
    canCustomizeExternalTcpPorts: KnockoutComputed<boolean>;
   
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

        this.showDnsInfo = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.showFullDomain = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.showAgreement = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.requirePublicIpWhenBindAllUsed = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.canCustomizeExternalIpsAndPorts = ko.pureComputed(() => this.model.mode() === "LetsEncrypt");
        this.canCustomizeExternalTcpPorts = ko.pureComputed(() => this.model.mode() === "Secured");
       
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
        
        this.updatePorts();
        
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
                firstNode.ips.push(new ipEntry(true));
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
            
            if (this.model.mode() === 'Secured') {
                if (!this.isValid(node.validationGroupForSecured)) {
                    validNodeConfig = false;
                }
            }
            
            if (this.model.mode() === 'LetsEncrypt') {
                if (!this.isValid(node.validationGroupForLetsEncrypt)) {
                    validNodeConfig = false;
                }
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
        const node = new nodeInfo(this.model.hostnameIsNotRequired, this.model.mode);
        this.model.nodes.push(node);
        this.editedNode(node);
        node.nodeTag(this.findFirstAvailableNodeTag());
        
        this.updatePorts();
        this.initTooltips();
    }
    
    private findFirstAvailableNodeTag() {
        for (let nodesTagsKey of serverSetup.nodesTags) {
            if (!this.model.nodes().find(x => x.nodeTag() === nodesTagsKey)) {
                return nodesTagsKey;
            }
        }
        
        return "";
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
        
        this.updatePorts();
    }
    
    updatePorts() {
        let idx = 0;
        this.model.nodes().forEach(node => {
           
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

        const ipText = ownCerts ? "IP Address or Hostname that should already be associated with DNS name in certificate." : 
                                  "IP Address or Hostname that will be associated with the DNS Name.";
        
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
                // This will be displayed only in 'Lets Encrypt' flow 
                content: ipAddressInfo + "<strong>Note:</strong> If Hostname is used then an external ip must also be provided.",
                placement: "top"
            });
        
        popoverUtils.longWithHover($("#https-port-info"), {
            content: "HTTPs port used for clients/browser (RavenDB Studio) communication.",
            placement: "top"
        });
        
        popoverUtils.longWithHover($("#tcp-port-info"), {
            content: "TCP port used by the cluster nodes to communicate with each other.",
            placement: "top"
        })
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
