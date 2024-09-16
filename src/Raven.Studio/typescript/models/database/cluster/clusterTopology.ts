
import clusterNode = require("models/database/cluster/clusterNode");
import { sortBy } from "common/typeUtils";

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
        
        const members = this.mapNodes("Member", topologyDto.Members, dto.Status, dto.NodeLicenseDetails);
        const promotables = this.mapNodes("Promotable", topologyDto.Promotables, dto.Status, dto.NodeLicenseDetails);
        const watchers = this.mapNodes("Watcher", topologyDto.Watchers, dto.Status, dto.NodeLicenseDetails);

        this.nodes(_.concat<clusterNode>(members, promotables, watchers));
        this.nodes(sortBy(this.nodes(), x => x.tag().toUpperCase()));

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
                     status: { [key: string]: Raven.Client.Http.NodeStatus; },
                     licenseDetails: { [key: string]: Raven.Server.Commercial.DetailsPerNode; }): Array<clusterNode> {
        return Object.entries(dict ?? []).map(([k, v]) => {
            // node statuses are available for all nodes except current
            const nodeStatus = status ? status[k] : null;
            const connected = nodeStatus ? nodeStatus.Connected : true;
            const errorDetails = connected ? null : nodeStatus.ErrorDetails;

            if (!this.anyErrorsEncountered() && !!errorDetails) {
                this.anyErrorsEncountered(true);
            }
            
            const node = clusterNode.for(k, v, type, connected, this.isPassive, errorDetails);
            
            if (licenseDetails && licenseDetails[k]) {
                const nodeLicenseDetails = licenseDetails[k];

                node.utilizedCores(nodeLicenseDetails.UtilizedCores);
                node.maxUtilizedCores(nodeLicenseDetails.MaxUtilizedCores);
                node.numberOfCores(nodeLicenseDetails.NumberOfCores);
                node.installedMemoryInGb(nodeLicenseDetails.InstalledMemoryInGb);
                node.usableMemoryInGb(nodeLicenseDetails.UsableMemoryInGb);

                const fullVersion = nodeLicenseDetails.BuildInfo ? nodeLicenseDetails.BuildInfo.FullVersion : null;
                node.nodeServerVersion(fullVersion);
                node.osInfo(nodeLicenseDetails.OsInfo);
            }
            
            return node;
        });
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged) {
        const newTopology = incomingChanges.Topology;

        const existingNodes = this.nodes();
        const newNodes: clusterNode[] = [].concat(
            this.mapNodes("Member", newTopology.Members, incomingChanges.Status, incomingChanges.NodeLicenseDetails),
            this.mapNodes("Promotable", newTopology.Promotables, incomingChanges.Status, incomingChanges.NodeLicenseDetails),
            this.mapNodes("Watcher", newTopology.Watchers, incomingChanges.Status, incomingChanges.NodeLicenseDetails)
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
                const locationToInsert = _.sortedIndexBy(this.nodes(), node, (item: clusterNode) => item.tag().toLowerCase());
                this.nodes.splice(locationToInsert, 0, node);
            }
        });

        this.nodeTag(incomingChanges.NodeTag);
        this.leader(incomingChanges.Leader);
        this.currentTerm(incomingChanges.CurrentTerm);
        this.currentState(incomingChanges.CurrentState);
    }
}

export = clusterTopology;
