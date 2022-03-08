import shard from "models/resources/shard";
import dialogViewModelBase from "viewmodels/dialogViewModelBase";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import database from "models/resources/database";


function transformUrlForNode(db: database, nodeTag: string, hash = window.location.hash) {
    const currentDatabase = activeDatabaseTracker.default.database();
    const clusterNode = clusterTopologyManager.default.getClusterNodeByTag(nodeTag);
    const serverUrl = clusterNode.serverUrl();

    if (hash.includes("database=") && currentDatabase) {
        const existingQueryString = "database=" + encodeURIComponent(currentDatabase.name);
        const newQueryString = "database=" + encodeURIComponent(db.name);
        hash = hash.replace(existingQueryString, newQueryString);
    }

    if (!serverUrl) {
        return "#";
    }

    return serverUrl + "/studio/index.html" + hash;
}

class shardSelector extends dialogViewModelBase {

    private readonly currentNode: KnockoutObservable<string>;
    private readonly shards: shard[];
    private readonly onLocalShardSelected: (shard: shard, pin: boolean) => void;

    targetIsLocal: KnockoutComputed<boolean>;
    targetHref: KnockoutComputed<string>;
    forcePin: KnockoutComputed<boolean>;
    
    private shardNodes: KnockoutComputed<string[]>;
    
    form = {
        shard: ko.observable<shard>(),
        node: ko.observable<string>(),
        pin: ko.observable<boolean>(false)
    } as const;
    
    view = require("views/common/sharding/shardSelector.html");
    
    constructor(shards: shard[], onLocalShardSelected: (shard: shard, pin: boolean) => void) {
        super();
        
        this.currentNode = clusterTopologyManager.default.localNodeTag;
        
        this.shards = shards;
        this.onLocalShardSelected = onLocalShardSelected;
        
        this.initForm();
        this.initObservables();
        
        this.bindToCurrentInstance("changeShard", "changeNodeTag", "shardSelected");
    }
    
    private initForm() {
        const activeDatabase = this.activeDatabase();
        const currentShard: shard = activeDatabase instanceof shard ? activeDatabase : null;
        this.form.shard(currentShard || this.shards[0]);
        this.maybeChangeNodeTag();
        this.form.pin(!!currentShard);
    }
    
    private initObservables() {
        this.shardNodes = ko.pureComputed(() => {
            const currentShard = this.form.shard();
            if (!currentShard) {
                return [];
            }

            return currentShard.nodes();
        });

        this.targetIsLocal = ko.pureComputed(() => {
            const currentNode = this.currentNode();
            const targetNode = this.form.node();
            return currentNode === targetNode;
        });

        this.targetHref = ko.pureComputed(() => {
            const db = this.form.shard();
            const nodeTag = this.form.node();
            return transformUrlForNode(db, nodeTag);
        });
        
        this.forcePin = ko.pureComputed(() => !this.targetIsLocal());
    }

    

    changeShard(shard: shard) {
        this.form.shard(shard);
        
        this.maybeChangeNodeTag();
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

    shardSelected(): boolean {
        if (this.targetIsLocal()) {
            this.onLocalShardSelected(this.form.shard(), this.forcePin() ? true : this.form.pin());
            return false;
        } else {
            // go to new tab - use build-in a href handling
            return true;
        }
    }
}

export = shardSelector;
