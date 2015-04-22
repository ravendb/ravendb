
import nodeConnectionInfo = require("models/database/cluster/nodeConnectionInfo");

class topology {

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
            var leaderNode = this.allNodes.first(n => n.name() === leader);
            if (leaderNode) {
                leaderNode.state("leader");
            }
        }
        
        this.topologyId(dto.TopologyId);
    }

}

export = topology;