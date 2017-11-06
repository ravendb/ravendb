/// <reference path="../../../typings/tsd.d.ts"/>

type configurationMode = "Unsecured" | "Secured" | "LetsEncrypt"; //tODO use enum SetupMode

import unsecureSetup = require("models/setup/unsecureSetup");
import licenseInfo = require("models/setup/licenseInfo");
import domainInfo = require("models/setup/domainInfo");
import nodeInfo = require("models/setup/nodeInfo");


class serverSetup {
    static default = new serverSetup();
    static readonly nodesTags = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'];

    mode = ko.observable<configurationMode>();
    license = ko.observable<licenseInfo>(new licenseInfo());
    domain = ko.observable<domainInfo>(new domainInfo());
    unsecureSetup = ko.observable<unsecureSetup>(new unsecureSetup());
    nodes = ko.observableArray<nodeInfo>();
    useOwnCertificates = ko.pureComputed(() => this.mode() && this.mode() === "Secured");

    nodesValidationGroup: KnockoutValidationGroup;

    constructor() {
        this.nodes.push(nodeInfo.empty(this.useOwnCertificates));

        this.nodes.extend({
            validation: [
                {
                    validator: () => this.nodes().length > 0,
                    message: "All least node is required"
                }
            ]
        });

        this.nodesValidationGroup = ko.validatedObservable({
            nodes: this.nodes
        });
    }

    toSecuredDto(): Raven.Server.Commercial.SetupInfo {
        const nodesInfo = {} as dictionary<Raven.Server.Commercial.SetupInfo.NodeInfo>;
        this.nodes().forEach((node, idx) => {
            const nodeTag = serverSetup.nodesTags[idx];
            nodesInfo[nodeTag] = node.toDto(); //TODO: check me!
        });

        return {
            License: this.license().toDto(),
            Email: null,
            Domain: this.domain().domain(),
            ModifyLocalServer: true,  //TODO: always true?
            NodeSetupInfos: nodesInfo
        };
    }
}

export = serverSetup;
