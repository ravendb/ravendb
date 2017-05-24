/// <reference path="../../../typings/tsd.d.ts"/>

import clusterTopology = require("models/database/cluster/clusterTopology");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");

class clusterTopologyManager {

    static default = new clusterTopologyManager();

    topology = ko.observable<clusterTopology>();

    nodeTag: KnockoutComputed<string>;

    init(): JQueryPromise<clusterTopology> {
        return this.forceRefresh();
    }

    constructor() {
        this.initObservables();

        setInterval(() => this.forceRefresh(), 1000); //TODO: dleete me!
    }

    //TODO: connect websocket updates - waitin for: RavenDB-6929

    //TODO: do we want to use this? 
    forceRefresh() {
        return new getClusterTopologyCommand(window.location.host)
            .execute()
            .done(topology => {
                this.topology(topology);
            });
        //TODO: handle failure
    }

    private initObservables() {
        this.nodeTag = ko.pureComputed(() => {
            const topology = this.topology();
            return topology ? topology.nodeTag() : null;
        });
    }
    
}

export = clusterTopologyManager;
