/// <reference path="../../../typings/tsd.d.ts"/>

type configurationMode = "Unsecured" | "Secured" | "LetsEncrypt"; //tODO use enum SetupMode

import unsecureSetup = require("models/setup/unsecureSetup");
import licenseInfo = require("models/setup/licenseInfo");
import domainInfo = require("models/setup/domainInfo");
import nodeInfo = require("models/setup/nodeInfo");


class serverSetup {
   static default = new serverSetup();
   
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
}

export = serverSetup;
