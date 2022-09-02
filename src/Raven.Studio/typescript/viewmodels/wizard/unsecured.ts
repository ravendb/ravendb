import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import popoverUtils = require("common/popoverUtils");
import ipEntry = require("models/wizard/ipEntry");
import nodeInfo = require("models/wizard/nodeInfo");
import serverSetup = require("models/wizard/serverSetup");
import studioConfigurationDatabaseModel = require("models/database/settings/studioConfigurationDatabaseModel");

class unsecured extends setupStep {

    view = require("views/wizard/unsecured.html");

    static environments = studioConfigurationDatabaseModel.environments;
    
    editedNode = ko.observable<nodeInfo>();

    remoteNodeIpOptions = ko.observableArray<string>(['0.0.0.0']);

    shouldDisplayUnsafeModeWarning: KnockoutComputed<boolean>;
    unsafeNetworkConfirm = ko.observable<boolean>(false);

    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        super();
        this.bindToCurrentInstance("removeNode", "editNode");

        this.initObservables();
        this.initValidation();
    }

    private initObservables() {
        this.shouldDisplayUnsafeModeWarning = ko.pureComputed(() =>
            this.model.nodes().some(node => node.ips().some(ip => ip.ip() && !ip.isLocalNetwork())));
    }
    
    private initValidation() {
        nodeInfo.setupNodeTagValidation(this.model.localNodeTag, {
            onlyIf: () => !this.model.startNodeAsPassive()
        });

        this.unsafeNetworkConfirm.extend({
            validation: [
                {
                    validator: () => {
                        return !this.shouldDisplayUnsafeModeWarning() || this.unsafeNetworkConfirm();
                    },
                    message: "Confirmation is required"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            unsafeNetworkConfirm: this.unsafeNetworkConfirm
        })
    }
    
    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "Unsecured") {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }

    activate(args: any) {
        super.activate(args);
        
        const initialIp = ipEntry.runningOnDocker ? "" : "127.0.0.1";
        
        this.model.nodes().forEach(node => {
            const firstIp = node.ips()[0].ip();

            if (!firstIp) {
                node.ips()[0] = ipEntry.forIp(initialIp, false);
            }
        });
    }
    
    compositionComplete() {
        super.compositionComplete();
        const nodes = this.model.nodes();

        if (nodes.length) {
            const firstNode = nodes[0];

            if (firstNode.ips().length === 0) {
                firstNode.ips.push(new ipEntry(true));
            }

            this.editedNode(firstNode);

            this.initTooltips();
        }
        
        this.initTooltips();
    }

    private initTooltips() {
        popoverUtils.longWithHover($("#passive-node"), {
            content: "<small>When the server is restarted this node will be in a Passive state, not part of a Cluster.</small>",
            placement: "bottom",
            html: true
        })

        popoverUtils.longWithHover($("#http-port-info"), {
            content: "<small>HTTP port used for clients/browser (RavenDB Studio) communication.</small>",
            html: true
        });

        popoverUtils.longWithHover($("#tcp-port-info"), {
            content: "<small>TCP port used by the cluster nodes to communicate with each other.</small>",
            html: true
        })
    }

    back() {
        router.navigate("#security");
    }
    
    save() {
        let isValid = true;
        let focusedOnInvalidNode = false;

        if (!this.isValid(this.validationGroup)) {
            isValid = false;
        }

        const nodes = this.model.nodes();
        nodes.forEach(node => {
            let validNodeConfig = true;
            
            if (!this.isValid(node.validationGroupForUnsecured)) {
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
        
        if (isValid) {
            router.navigate("#finish");
        }
    }

    addNode() {
        const node = new nodeInfo(this.model.hostnameIsNotRequired, this.model.mode);
        this.model.nodes.push(node);
        
        if (this.model.nodes().length > 0) {
            this.model.startNodeAsPassive(false);
        }
        
        this.editedNode(node);
        node.nodeTag(this.findFirstAvailableNodeTag());

        this.updatePorts();
        this.initTooltips();
    }
   
    private findFirstAvailableNodeTag() {
        for (const nodesTagsKey of serverSetup.nodesTags) {
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
        
        if (this.model.nodes().length === 1) {
            this.editNode(this.model.nodes()[0]);
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
}

export = unsecured;
