import shard from "models/resources/shard";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";

class inlineShardSelector {

    private readonly db: database;
    private readonly currentNode: KnockoutObservable<string>;
    private readonly shards: shard[];
    
    private readonly dropup: boolean;

    private shardNodes: KnockoutComputed<string[]>;
    
    form = {
        shard: ko.observable<shard>(),
        node: ko.observable<string>(),
    } as const;
    
    view = require("views/common/sharding/inlineShardSelector.html");

    getView() {
        return this.view;
    }
    
    constructor(db: database, opts: { dropup?: boolean} = {}) {
        this.db = db;
        this.currentNode = clusterTopologyManager.default.localNodeTag;
        this.dropup = opts.dropup ?? false;

        const rootDb = db.root;
        this.shards = (rootDb instanceof shardedDatabase) ? rootDb.shards() : [];
        
        this.initForm();
        this.initObservables();
        
        _.bindAll(this, "changeShard", "changeNodeTag");
    }
    
    private initForm() {
        const currentShard: shard = this.db instanceof shard ? this.db : null;
        this.form.shard(currentShard || this.shards[0]);
        this.maybeChangeNodeTag();
    }
    
    private initObservables() {
        this.shardNodes = ko.pureComputed(() => {
            const currentShard = this.form.shard();
            if (!currentShard) {
                return [];
            }

            return currentShard.nodes().map(x => x.tag);
        });
        
        this.form.shard.subscribe(() => this.maybeChangeNodeTag());
    }

    changeShard(shard: shard) {
        this.form.shard(shard);
    }
    
    location(): databaseLocationSpecifier {
        return {
            nodeTag: this.form.node(),
            shardNumber: this.form.shard().shardNumber
        }
    }
    
    private maybeChangeNodeTag() {
        const currentServerNodeTag = this.currentNode();

        // try to find and select current currentServerNodeTag (if available) - to prefer local reads
        const canSelectCurrentNode = !!this.form.shard().nodes().find(x => x.tag === currentServerNodeTag);
        if (canSelectCurrentNode) {
            this.form.node(currentServerNodeTag);
        } else {
            const canUseCurrentNode = !!this.form.shard().nodes().find(x => x.tag === this.form.node());
            if (canUseCurrentNode) {
                // nothing to be done - don't touch node tag
            } else {
                // keep UI in sync - select first from the list
                this.form.node(this.form.shard().nodes()[0].tag);
            }
        }
    }
    
    changeNodeTag(tag: string): void {
        this.form.node(tag);
    }
    
}

export = inlineShardSelector;
