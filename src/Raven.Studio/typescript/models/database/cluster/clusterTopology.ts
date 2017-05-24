
import clusterNode = require("models/database/cluster/clusterNode");

class clusterTopology {

    leader = ko.observable<string>();
    nodeTag = ko.observable<string>();

    nodes = ko.observableArray<clusterNode>([]);

    constructor(dto: clusterTopologyDto) {
        this.leader(dto.Leader);
        this.nodeTag(dto.NodeTag);

        const topologyDto = dto.Topology;

        const members = this.mapNodes("Member", topologyDto.Members);
        const promotables = this.mapNodes("Promotable", topologyDto.Promotables);
        const watchers = this.mapNodes("Watcher", topologyDto.Watchers);

        this.nodes(_.concat<clusterNode>(members, promotables, watchers));
    }

    private mapNodes(type: clusterNodeType, dict: System.Collections.Generic.Dictionary<string, string>): Array<clusterNode> {
        return _.map(dict, (v, k) => clusterNode.for(k, v, type));
    }

    updateWith(incomingChanges: clusterTopologyDto) {
        const newTopology = incomingChanges.Topology;

        const existingNodes = this.nodes();
        const newNodes = _.concat<clusterNode>(
            this.mapNodes("Member", newTopology.Members),
            this.mapNodes("Promotable", newTopology.Promotables),
            this.mapNodes("Watcher", newTopology.Watchers)
        );
        const newServerUrls = new Set(newNodes.map(x => x.serverUrl()));

        const toDelete = existingNodes.filter(x => !newServerUrls.has(x.serverUrl()));
        toDelete.forEach(x => this.nodes.remove(x));

        newNodes.forEach(node => {
            const matchedNode = existingNodes.find(x => x.serverUrl() === node.serverUrl());

            if (matchedNode) {
                matchedNode.updateWith(node);
            } else {
                this.nodes.push(node);
            }
        });

        this.nodeTag(incomingChanges.NodeTag);
        this.leader(incomingChanges.Leader);
    }

    /*TODO:
    currentLeader = ko.observable<string>();
    currentTerm = ko.observable<number>();
    state = ko.observable<string>();
    commitIndex = ko.observable<number>();
    allNodes = ko.observableArray<nodeConnectionInfo>();
    allVotingNodes = ko.observableArray<nodeConnectionInfo>();
    promotableNodes = ko.observableArray<nodeConnectionInfo>();
    nonVotingNodes = ko.observableArray<nodeConnectionInfo>();
    topologyId = ko.observable<string>();

    clusterMode = ko.computed(() => {
        var voting = this.allVotingNodes().length;
        var promatable = this.promotableNodes().length;
        var nonVoting = this.nonVotingNodes().length;

        return voting + promatable + nonVoting > 0;
    });

    constructor(dto: topologyDto) {
        this.currentLeader(dto.CurrentLeader);
        this.currentTerm(dto.CurrentTerm);
        this.state(dto.State);
        this.commitIndex(dto.CommitIndex);

        dto.AllVotingNodes.map(n => {
            var nci = new nodeConnectionInfo(n);
            nci.state("voting");
            this.allVotingNodes.push(nci);
            this.allNodes.push(nci);
        });

        dto.PromotableNodes.map(n => {
            var nci = new nodeConnectionInfo(n);
            nci.state("promotable");
            this.promotableNodes.push(nci);
            this.allNodes.push(nci);
        });

        dto.NonVotingNodes.map(n => {
            var nci = new nodeConnectionInfo(n);
            nci.state("non-voting");
            this.nonVotingNodes.push(nci);
            this.allNodes.push(nci);
        });

        var leader = this.currentLeader();
        if (leader) {
            const leaderNode = this.allNodes().find(n => n.name() === leader);
            if (leaderNode) {
                leaderNode.state("leader");
            }
        }
        
        this.topologyId(dto.TopologyId);
    }*/

}

export = clusterTopology;
