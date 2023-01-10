import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import addShardToDatabaseGroupCommand from "commands/database/dbGroup/addShardToDatabaseGroupCommand";

interface nodeInfo {
    tag: string;
    type: databaseGroupNodeType;
}

class addNewShardToDatabaseGroup extends dialogViewModelBase {
    
    view = require("views/resources/addNewShardToDatabaseGroup.html");
    
    databaseName: string;
    validationGroup: KnockoutValidationGroup;
    
    replicationFactor = ko.observable<number>(3);
    
    spinners = {
        addShard: ko.observable<boolean>(false)
    };

    constructor(databaseName: string) {
        super();
        
        this.databaseName = databaseName;
        
        this.initValidation();
    }
    
    private initValidation() {
        this.replicationFactor.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            replicationFactor: this.replicationFactor,
        });
    }
    
    activate() {
        return true;
    }

    addShard() {
        if (this.isValid(this.validationGroup)) {
            this.spinners.addShard(true);

            new addShardToDatabaseGroupCommand(this.databaseName, this.replicationFactor())
                .execute()
                .done(() => {
                    this.close();
                })
                .always(() => this.spinners.addShard(false));
        }
    }
}

export = addNewShardToDatabaseGroup;
