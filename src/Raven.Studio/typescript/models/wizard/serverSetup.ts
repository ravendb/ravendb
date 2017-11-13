/// <reference path="../../../typings/tsd.d.ts"/>
import unsecureSetup = require("models/wizard/unsecureSetup");
import licenseInfo = require("models/wizard/licenseInfo");
import domainInfo = require("models/wizard/domainInfo");
import nodeInfo = require("models/wizard/nodeInfo");
import certificateInfo = require("models/wizard/certificateInfo");

class serverSetup {
    static default = new serverSetup();
    static readonly nodesTags = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'];

    userDomains = ko.observable<Raven.Server.Commercial.UserDomainsWithIps>();
    
    mode = ko.observable<Raven.Server.Commercial.SetupMode>();
    license = ko.observable<licenseInfo>(new licenseInfo());
    domain = ko.observable<domainInfo>(new domainInfo(() => this.license().toDto()));
    unsecureSetup = ko.observable<unsecureSetup>(new unsecureSetup());
    nodes = ko.observableArray<nodeInfo>();
    certificate = ko.observable<certificateInfo>(new certificateInfo());
    registerClientCertificate = ko.observable<boolean>(true);
    
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
        const newNode = new nodeInfo(this.hostnameIsNotRequired);
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
                    message: "All least node is required"
                }
            ]
        });

        this.nodesValidationGroup = ko.validatedObservable({
            nodes: this.nodes,
        });
    }

    private getPortPart() {
        const port = this.nodes()[0].port();
        return port && port !== "443" ? ":" + port : "";
    }

    toSecuredDto(): Raven.Server.Commercial.SetupInfo {
        const nodesInfo = {} as dictionary<Raven.Server.Commercial.SetupInfo.NodeInfo>;
        this.nodes().forEach((node, idx) => {
            const nodeTag = serverSetup.nodesTags[idx];
            nodesInfo[nodeTag] = node.toDto();
        });

        return {
            License: this.license().toDto(),
            Email: this.domain().userEmail(),
            Domain: this.domain().domain(),
            ModifyLocalServer: true,
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
        return this.certificate().certificateCNs()[0].replace("*", "a");
    }

    getStudioUrl() {
        switch (this.mode()) {
            case "Unsecured":
                const publicUrl = this.unsecureSetup().publicServerUrl();
                
                if (publicUrl) {
                    return publicUrl;
                }
                const portPart = this.unsecureSetup().port() || '8080';
                return "http://" + this.unsecureSetup().ips()[0].ip() + ':' + portPart; 
            case "LetsEncrypt":
                return "https://a." + this.domain().domain() + ".dbs.local.ravendb.net" + this.getPortPart();
            case "Secured":
                const wildcard = this.certificate().wildcardCertificate();
                if (wildcard) {
                    const domain = this.getDomainForWildcard("a");
                    return "https://" + domain + this.getPortPart();
                } else {
                    return this.nodes()[0].getServerUrl();
                }
            default:
                return null;
        }
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
                        return this.getDomainForWildcard(tag);
                    } else {
                        return tag;
                    }
                default:
                    return null;
            }
        });
    }
}

export = serverSetup;
