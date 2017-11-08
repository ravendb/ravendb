/// <reference path="../../../typings/tsd.d.ts"/>

import unsecureSetup = require("models/setup/unsecureSetup");
import licenseInfo = require("models/setup/licenseInfo");
import domainInfo = require("models/setup/domainInfo");
import nodeInfo = require("models/setup/nodeInfo");
import certificateInfo = require("models/setup/certificateInfo");

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

    useOwnCertificates = ko.pureComputed(() => this.mode() && this.mode() === "Secured");

    nodesValidationGroup: KnockoutValidationGroup;

    constructor() {
        const newNode = new nodeInfo();
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
            NodeSetupInfos: nodesInfo,
            Certificate: this.certificate().certificate(),
            Password: this.certificate().certificatePassword()
        };
    }
}

export = serverSetup;
