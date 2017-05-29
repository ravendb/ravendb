/// <reference path="../../../typings/tsd.d.ts"/>

import clusterTopology = require("models/database/cluster/clusterTopology");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import changesContext = require("common/changesContext");

class clusterTopologyManager {

    static default = new clusterTopologyManager();

    topology = ko.observable<clusterTopology>();

    nodeTag: KnockoutComputed<string>;

    nodesCount: KnockoutComputed<number>;

    init(): JQueryPromise<clusterTopology> {
        return this.fetchTopology();
    }

    private fetchTopology() {
        return new getClusterTopologyCommand()
            .execute()
            .done(topology => {
                this.topology(topology);
            });
    }

    constructor() {
        this.initObservables();
    }

    setupGlobalNotifications() {
        const serverWideClient = changesContext.default.serverNotifications();

        serverWideClient.watchClusterTopologyChanges(e => this.onTopologyUpdated(e));
        serverWideClient.watchReconnect(() => this.fetchTopology());
    }

    private onTopologyUpdated(e: Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged) {
        this.topology().updateWith(e);
    }

    private initObservables() {
        this.nodeTag = ko.pureComputed(() => {
            const topology = this.topology();
            return topology ? topology.nodeTag() : null;
        });

        this.nodesCount = ko.pureComputed(() => {
            const topology = this.topology();
            return topology ? topology.nodes().length : 0;
        });
    }
    
}

export = clusterTopologyManager;
