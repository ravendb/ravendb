import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import addOrchestratorToDatabaseGroupCommand from "commands/database/dbGroup/addOrchestratorToDatabaseGroupCommand";

interface nodeInfo {
    tag: string;
    type: databaseGroupNodeType;
}

class addNewOrchestratorToDatabaseGroup extends dialogViewModelBase {

    view = require("views/resources/addNewOrchestratorToDatabaseGroup.html");

    nodeTag = ko.observable<string>();

    databaseName: string;
    nodes: nodeInfo[];
    
    validationGroup: KnockoutValidationGroup;
    
    nodesCanBeAdded: KnockoutComputed<string[]>;

    spinners = {
        addNode: ko.observable<boolean>(false)
    };

    constructor(databaseName: string, nodes: nodeInfo[]) {
        super();
        
        this.databaseName = databaseName;
        this.nodes = nodes;
        
        this.bindToCurrentInstance("selectedClusterNode");
        
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables() {
        this.nodesCanBeAdded = ko.pureComputed<string[]>(() => {
            const tags = clusterTopologyManager.default.topology().nodes().map(x => x.tag());
            const existingTags = this.nodes.map(x => x.tag);
            return tags.filter(x => !existingTags.includes(x));
        });
    }
    
    private initValidation() {
        this.nodeTag.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            nodeTag: this.nodeTag,
        });
    }
    
    activate() {
        return true;
    }
    
    addNode() {
        if (this.isValid(this.validationGroup)) {
            this.spinners.addNode(true);

            new addOrchestratorToDatabaseGroupCommand(this.databaseName, this.nodeTag())
                .execute()
                .done(() => {
                    this.close();
                })
                .always(() => this.spinners.addNode(false));
        }
    }

    selectedClusterNode(node: string) {
        this.nodeTag(node);
    }
}

export = addNewOrchestratorToDatabaseGroup;
