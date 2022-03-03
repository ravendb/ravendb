import shard from "models/resources/shard";
import dialogViewModelBase from "viewmodels/dialogViewModelBase";

class shardSelector extends dialogViewModelBase {

    private readonly currentNode: string;
    private readonly shards: shard[];
    private readonly onSelected: (shard: shard, nodeTag: string, pin: boolean) => void;
    
    private readonly shardNodes: KnockoutComputed<string[]>;
    
    form = {
        shard: ko.observable<shard>(),
        node: ko.observable<string>(),
        pin: ko.observable<boolean>(false)
    } as const;
    
    view = require("views/common/sharding/shardSelector.html");
    
    constructor(shards: shard[], onSelected: (shard: shard, nodeTag: string, pin: boolean) => void) {
        super();
        
        this.shards = shards;
        this.onSelected = onSelected;
        
        this.form.shard(shards[0]); //TODO: try default to current value, not first one 
        this.form.node("A"); //TODO: 
        this.form.pin(this.activeDatabase() instanceof shard);
        
        this.shardNodes = ko.pureComputed(() => {
            const currentShard = this.form.shard();
            if (!currentShard) {
                return [];
            }
            
            return currentShard.nodes();
        });
        
        this.bindToCurrentInstance("changeShard", "changeNodeTag", "shardSelected");
    }

    changeShard(shard: shard) {
        this.form.shard(shard);
    }
    
    changeNodeTag(tag: string) {
        this.form.node(tag);
    }

    shardSelected() {
        this.onSelected(this.form.shard(), this.form.node(), this.form.pin());
    }
    
}

export = shardSelector;
