import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import addShardToDatabaseGroupCommand from "commands/database/dbGroup/addShardToDatabaseGroupCommand";
import getClusterTopologyCommand from "commands/database/cluster/getClusterTopologyCommand";
import clusterNode from "models/database/cluster/clusterNode";

class addNewShardToDatabaseGroup extends dialogViewModelBase {
    
    view = require("views/resources/addNewShardToDatabaseGroup.html");
    
    databaseName: string;
    validationGroup: KnockoutValidationGroup;
    
    replicationFactor = ko.observable<number>();

    manualMode = ko.observable<boolean>(false);

    clusterNodes: clusterNode[] = [];
    
    nodes = ko.observableArray<clusterNode>([]);

    selectionState: KnockoutComputed<checkbox>;
    
    spinners = {
        addShard: ko.observable<boolean>(false)
    };

    constructor(databaseName: string) {
        super();
        
        this.databaseName = databaseName;
        
        this.manualMode.subscribe(manual => {
            if (manual) {
                this.replicationFactor(this.nodes().length);
            }
        });
        
        this.nodes.subscribe(nodes => {
            if (this.manualMode()) {
                this.replicationFactor(nodes.length);
            }
        });

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const clusterNodes = this.clusterNodes;
            const selectedCount = this.nodes().length;

            if (clusterNodes.length && selectedCount === clusterNodes.length)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });
        
        this.initValidation();
    }
    
    private initValidation() {
        this.replicationFactor.extend({
            required: true,
            min: 1
        });

        this.validationGroup = ko.validatedObservable({
            replicationFactor: this.replicationFactor,
        });
    }
    
    activate() {
        return new getClusterTopologyCommand()
            .execute()
            .done(topology => {
                this.clusterNodes = topology.nodes();
                this.replicationFactor(this.clusterNodes.length);
            });
    }

    addShard() {
        if (this.isValid(this.validationGroup)) {
            this.spinners.addShard(true);

            new addShardToDatabaseGroupCommand(this.databaseName, this.replicationFactor(), this.manualMode() ? this.nodes().map(x => x.tag()) : null)
                .execute()
                .done(() => {
                    this.close();
                })
                .always(() => this.spinners.addShard(false));
        }
    }

    toggleSelectAll() {
        const selectedCount = this.nodes().length;

        if (selectedCount > 0) {
            this.nodes([]);
        } else {
            this.nodes(this.clusterNodes.slice());
        }
    }
}

export = addNewShardToDatabaseGroup;
