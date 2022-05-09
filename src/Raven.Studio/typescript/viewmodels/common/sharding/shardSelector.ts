import shard from "models/resources/shard";
import dialogViewModelBase from "viewmodels/dialogViewModelBase";
import clusterTopologyManager from "common/shell/clusterTopologyManager";

class shardSelector extends dialogViewModelBase {

    private readonly currentNode: KnockoutObservable<string>;
    private readonly shards: shard[];
    private readonly onShardSelected: (shard: shard, nodeTag: string) => void;

    private shardNodes: KnockoutComputed<string[]>;
    
    form = {
        shard: ko.observable<shard>(),
        node: ko.observable<string>(),
    } as const;
    
    view = require("views/common/sharding/shardSelector.html");
    
    constructor(shards: shard[], onShardSelected: (shard: shard, nodeTag: string) => void) {
        super();
        
        this.currentNode = clusterTopologyManager.default.localNodeTag;
        
        this.shards = shards;
        this.onShardSelected = onShardSelected;
        
        this.initForm();
        this.initObservables();
        
        this.bindToCurrentInstance("changeShard", "changeNodeTag", "shardSelected");
    }
    
    private initForm() {
        const activeDatabase = this.activeDatabase();
        const currentShard: shard = activeDatabase instanceof shard ? activeDatabase : null;
        this.form.shard(currentShard || this.shards[0]);
        this.maybeChangeNodeTag();
    }
    
    private initObservables() {
        this.shardNodes = ko.pureComputed(() => {
            const currentShard = this.form.shard();
            if (!currentShard) {
                return [];
            }

            return currentShard.nodes();
        });
        
        this.form.shard.subscribe(() => this.maybeChangeNodeTag());
    }

    changeShard(shard: shard) {
        this.form.shard(shard);
    }
    
    private maybeChangeNodeTag() {
        const currentServerNodeTag = this.currentNode();

        // try to find and select current currentServerNodeTag (if available) - to prefer local reads
        const canSelectCurrentNode = !!this.form.shard().nodes().find(x => x === currentServerNodeTag);
        if (canSelectCurrentNode) {
            this.form.node(currentServerNodeTag);
        } else {
            const canUseCurrentNode = !!this.form.shard().nodes().find(x => x === this.form.node());
            if (canUseCurrentNode) {
                // nothing to be done - don't touch node tag
            } else {
                // keep UI in sync - select first from the list
                this.form.node(this.form.shard().nodes()[0]);
            }
        }
    }
    
    changeNodeTag(tag: string): void {
        this.form.node(tag);
    }

    shardSelected() {
        this.onShardSelected(this.form.shard(), this.form.node());
    }
}

export = shardSelector;
