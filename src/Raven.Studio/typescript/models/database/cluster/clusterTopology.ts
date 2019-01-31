
import clusterNode = require("models/database/cluster/clusterNode");

class clusterTopology {
    leader = ko.observable<string>();
    nodeTag = ko.observable<string>();
    currentState = ko.observable<Raven.Client.ServerWide.RachisState>();
    currentTerm = ko.observable<number>();
    
    isPassive = ko.pureComputed(() => this.currentState() === "Passive");
    
    nodes = ko.observableArray<clusterNode>([]);
    
    membersCount: KnockoutComputed<number>;
    promotablesCount: KnockoutComputed<number>;
    watchersCount: KnockoutComputed<number>;

    anyErrorsEncountered = ko.observable<boolean>(false);

    constructor(dto: Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged) {
        this.leader(dto.Leader);
        this.nodeTag(dto.NodeTag);
        this.currentTerm(dto.CurrentTerm);
        this.currentState(dto.CurrentState);

        const topologyDto = dto.Topology;

        const members = this.mapNodes("Member", topologyDto.Members, dto.Status);
        const promotables = this.mapNodes("Promotable", topologyDto.Promotables, dto.Status);
        const watchers = this.mapNodes("Watcher", topologyDto.Watchers, dto.Status);

        this.nodes(_.concat<clusterNode>(members, promotables, watchers));
        this.nodes(_.sortBy(this.nodes(), x => x.tag().toUpperCase()));

        this.updateNodeDetails(dto.NodeLicenseDetails);
        
        this.membersCount = ko.pureComputed(() => {
            const nodes = this.nodes();
            return nodes.filter(x => x.type() === "Member").length;
        });

        this.promotablesCount = ko.pureComputed(() => {
            const nodes = this.nodes();
            return nodes.filter(x => x.type() === "Promotable").length;
        });

        this.watchersCount = ko.pureComputed(() => {
            const nodes = this.nodes();
            return nodes.filter(x => x.type() === "Watcher").length;
        });

        this.nodes().forEach(node => {      
            node.isLeader(node.tag() === this.leader());
        });
    }

    private mapNodes(type: clusterNodeType, dict: System.Collections.Generic.Dictionary<string, string>,
        status: { [key: string]: Raven.Client.Http.NodeStatus; }): Array<clusterNode> {
        return _.map(dict, (v, k) => {
            // node statuses are available for all nodes except current
            const nodeStatus = status ? status[k] : null;
            const connected = nodeStatus ? nodeStatus.Connected : true;
            const errorDetails = connected ? null : nodeStatus.ErrorDetails;

            if (!this.anyErrorsEncountered() && !!errorDetails) {
                this.anyErrorsEncountered(true);
            }
            
            return clusterNode.for(k, v, type, connected, this.isPassive, errorDetails);
        });
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged) {
        const newTopology = incomingChanges.Topology;

        const existingNodes = this.nodes();
        const newNodes = _.concat<clusterNode>(
            this.mapNodes("Member", newTopology.Members, incomingChanges.Status),
            this.mapNodes("Promotable", newTopology.Promotables, incomingChanges.Status),
            this.mapNodes("Watcher", newTopology.Watchers, incomingChanges.Status)
        );
        const newServerUrls = new Set<string>(newNodes.map(x => x.serverUrl()));

        const toDelete = existingNodes.filter(x => !newServerUrls.has(x.serverUrl()));
        toDelete.forEach(x => this.nodes.remove(x));

        newNodes.forEach(node => {
            node.isLeader(node.tag() === incomingChanges.Leader);
            
            const matchedNode = existingNodes.find(x => x.serverUrl() === node.serverUrl());           
                        
            if (matchedNode) {
                matchedNode.updateWith(node);
            } else {
                const locationToInsert = _.sortedIndexBy(this.nodes(), node, item => item.tag().toLowerCase());
                this.nodes.splice(locationToInsert, 0, node);
            }
        });

        this.updateNodeDetails(incomingChanges.NodeLicenseDetails);
        this.nodeTag(incomingChanges.NodeTag);
        this.leader(incomingChanges.Leader);
        this.currentTerm(incomingChanges.CurrentTerm);
        this.currentState(incomingChanges.CurrentState);
    }

    private updateNodeDetails(nodeLicenseDetails: { [key: string]: Raven.Server.Commercial.DetailsPerNode; }) {
        if (!nodeLicenseDetails)
            return;

        _.forOwn(nodeLicenseDetails, (detailsPerNode, nodeTag) => {
            const node = this.nodes().find(x => x.tag() === nodeTag);
            if (!node) {
                return;
            }

            node.utilizedCores(detailsPerNode.UtilizedCores);
            node.numberOfCores(detailsPerNode.NumberOfCores);
            node.installedMemoryInGb(detailsPerNode.InstalledMemoryInGb);
            node.usableMemoryInGb(detailsPerNode.UsableMemoryInGb);
            
            const fullVersion = detailsPerNode.BuildInfo ? detailsPerNode.BuildInfo.FullVersion : null;
            node.nodeServerVersion(fullVersion);
            node.osInfo(detailsPerNode.OsInfo);
        });
    }
}

export = clusterTopology;
