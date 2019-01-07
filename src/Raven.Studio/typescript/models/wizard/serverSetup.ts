/// <reference path="../../../typings/tsd.d.ts"/>
import unsecureSetup = require("models/wizard/unsecureSetup");
import licenseInfo = require("models/wizard/licenseInfo");
import domainInfo = require("models/wizard/domainInfo");
import nodeInfo = require("models/wizard/nodeInfo");
import continueSetup = require("models/wizard/continueSetup");
import certificateInfo = require("models/wizard/certificateInfo");
import ipEntry = require("models/wizard/ipEntry");
import setupShell = require("viewmodels/wizard/setupShell");

class serverSetup {
    static default = new serverSetup();
    static readonly nodesTags = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'];

    userDomains = ko.observable<Raven.Server.Commercial.UserDomainsWithIps>();
    
    useExperimentalFeatures = ko.observable<boolean>(false);
    
    mode = ko.observable<Raven.Server.Commercial.SetupMode | "Continue">();
    license = ko.observable<licenseInfo>(new licenseInfo());
    domain = ko.observable<domainInfo>(new domainInfo(() => this.license().toDto()));
    unsecureSetup = ko.observable<unsecureSetup>(new unsecureSetup());
    continueSetup = ko.observable<continueSetup>(new continueSetup());
    nodes = ko.observableArray<nodeInfo>();
    certificate = ko.observable<certificateInfo>(new certificateInfo());
    registerClientCertificate = ko.observable<boolean>(true);
    agreementUrl = ko.observable<string>();

    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>("None");
    
    fixedLocalPort = ko.observable<number>();
    fixPortNumberOnLocalNode = ko.pureComputed(() => this.fixedLocalPort() != null);
    fixedTcpPort = ko.observable<number>();
    fixTcpPortNumberOnLocalNode = ko.pureComputed(() => this.fixedTcpPort() != null);
    
    localIps = ko.observableArray<string>([]);
    
    useOwnCertificates = ko.pureComputed(() => this.mode() && this.mode() === "Secured");
    hostnameIsNotRequired = ko.pureComputed(() => {
        if (this.mode() !== "Secured") {
            return true;
        }
        
        return this.certificate().wildcardCertificate();
    });

    nodesValidationGroup: KnockoutValidationGroup;

    constructor() {
        const newNode = new nodeInfo(this.hostnameIsNotRequired, this.mode);
        newNode.nodeTag("A");
        this.nodes.push(newNode);

        this.certificate.extend({
            required: {
                onlyIf: () => this.useOwnCertificates()
            }
        });

        this.certificate.extend({
            required: {
                onlyIf: () => this.useOwnCertificates()
            }
        });
        
        this.nodes.extend({
            validation: [
                {
                    validator: () => this.nodes().length > 0,
                    message: "At least one node is required"
                }
            ]
        });

        this.nodesValidationGroup = ko.validatedObservable({
            nodes: this.nodes,
        });
    }
    
    init(params: Raven.Server.Commercial.SetupParameters) {
        if (params.FixedServerPortNumber != null) {
            this.fixedLocalPort(params.FixedServerPortNumber);
            
            this.unsecureSetup().port(this.fixedLocalPort().toString());
        } else {
            this.fixedLocalPort(null);
        }
        
        if (params.FixedServerTcpPortNumber != null) {
            this.fixedTcpPort(params.FixedServerTcpPortNumber);
            
            this.unsecureSetup().tcpPort(this.fixedTcpPort().toString());
        } else {
            this.fixedTcpPort(null);
        }
        
        ipEntry.runningOnDocker = params.IsDocker;

        if (params.IsAzure) {
            setupShell.deploymentEnvironment("Azure");
        } else if (params.IsAws) {
            setupShell.deploymentEnvironment(params.RunningOnPosix ? "AwsLinux" : "AwsWindows");
        } else {
            setupShell.deploymentEnvironment("Custom");
        }
    }

    private getLocalNode() {
        return this.nodes()[0];
    }
    
    private getPortPart() {
        const port = this.nodes()[0].port();
        return port && port !== "443" ? ":" + port : "";
    }

    toContinueSetupDto() {
        return {
            NodeTag: this.continueSetup().nodeTag(),
            Zip: this.continueSetup().zipFile(),
            RegisterClientCert: this.registerClientCertificate()
        } as Raven.Server.Commercial.ContinueSetupInfo;
    }

    toUnsecuredDto() : Raven.Server.Commercial.UnsecuredSetupInfo {
        const setup = this.unsecureSetup();
        return {
            EnableExperimentalFeatures: this.useExperimentalFeatures(),
            Port: setup.port() ? parseInt(setup.port(), 10) : 8080,
            Addresses: [setup.ip().ip()],
            LocalNodeTag: setup.bootstrapCluster() ? setup.localNodeTag() : null,
            Environment: setup.bootstrapCluster() ? this.environment() : null,
            TcpPort: setup.tcpPort() ? parseInt(setup.tcpPort(), 10) : 38888
        }
    }
    
    toSecuredDto(): Raven.Server.Commercial.SetupInfo {
        const nodesInfo = {} as dictionary<Raven.Server.Commercial.SetupInfo.NodeInfo>;
        this.nodes().forEach((node, idx) => {
            nodesInfo[node.nodeTag()] = node.toDto();
        });

        return {
            EnableExperimentalFeatures: this.useExperimentalFeatures(),
            Environment: this.environment(),
            License: this.license().toDto(),
            Email: this.domain().userEmail(),
            Domain: this.domain().domain(),
            RootDomain: this.domain().rootDomain(),
            ModifyLocalServer: true,
            LocalNodeTag: this.getLocalNode().nodeTag(),
            RegisterClientCert: this.registerClientCertificate(), 
            NodeSetupInfos: nodesInfo,
            Certificate: this.certificate().certificate(),
            Password: this.certificate().certificatePassword()
        };
    }
    
    private getDomainForWildcard(tag: string) {
        if (this.certificate().certificateCNs().length === 0) {
            return "";
        }

        const cn = this.certificate().certificateCNs()[0];
        
        if (!tag) {
            return cn.replace("*.", "");
        }
        return cn.replace("*", tag);
    }

    getStudioUrl() {
        switch (this.mode()) {
            case "Continue":
                return this.continueSetup().serverUrl();
            case "Unsecured":
                const setupPort = this.unsecureSetup().port() || '8080';
                const setupAddress = this.unsecureSetup().ip().ip();
                let host, 
                    port = setupPort;
                if (setupAddress === "0.0.0.0") {
                    host = document.location.hostname;
                } else {
                    host = setupAddress;
                }

                return `http://${host}:${port}`;
                
            case "LetsEncrypt":
                return "https://" + this.getLocalNode().nodeTag().toLocaleLowerCase() + "." + this.domain().domain() + "." + this.domain().rootDomain() + this.getPortPart();
                
            case "Secured":
                const wildcard = this.certificate().wildcardCertificate();
                if (wildcard) {
                    const domain = this.getDomainForWildcard(this.getLocalNode().nodeTag().toLocaleLowerCase());
                    return "https://" + domain + this.getPortPart();
                } else {
                    return this.nodes()[0].getServerUrl();
                }
                
            default:
                return null;
        }
    }
    
    createIsLocalNodeObservable(node: nodeInfo) {
        return ko.pureComputed(() => {
            return this.nodes().indexOf(node) === 0;
        });
    }
    
    createFullNodeNameObservable(node: nodeInfo) {
        return ko.pureComputed(() => {
            const tag = node.nodeTag();
            if (!tag) {
                return "";
            }
            
            const mode = this.mode();
            switch (mode) {
                case "LetsEncrypt":
                    return this.domain().fullDomain().toLocaleLowerCase();
                    
                case "Secured":
                    const wildcard = this.certificate().wildcardCertificate();
                    
                    if (wildcard) {
                        return this.getDomainForWildcard(null);
                    } else {
                        return node.hostname() || "<select hostname below>";
                    }
                default:
                    return null;
            }
        });
    }

    createIpAutocompleter(usedIps: KnockoutObservableArray<ipEntry>, ip: KnockoutObservable<string>) {
        const ips = usedIps || ko.observableArray<ipEntry>();
        return ko.pureComputed(()=> {
            const key = ip();
            
            const options = this.localIps();            
            const usedOptions = ips().filter(k => k.ip() !== key).map(x => x.ip());
            
            // here we don't take ip variable into account, so user can easily change 
            // from 127.0.0.1 to 192.168.0.1 etc.            
            return _.difference(options, usedOptions);
        });
    }
}

export = serverSetup;
