import database from "models/resources/database";
import shard from "models/resources/shard";
import StudioDatabaseInfo = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabases.StudioDatabaseInfo;
import { NonShardedDatabaseInfo, ShardedDatabaseInfo } from "components/models/databases";
import DeletionInProgressStatus = Raven.Client.ServerWide.DeletionInProgressStatus;

class shardedDatabase extends database {
    
    shards = ko.observableArray<shard>([]);

    isSharded(): this is shardedDatabase {
        return true;
    }
    
    constructor(dbInfo: StudioDatabaseInfo, clusterNodeTag: KnockoutObservable<string>) {
        super(dbInfo, clusterNodeTag);
        
        this.updateUsing(dbInfo);
    }

    get root(): database {
        return this;
    }

    getLocations(): databaseLocationSpecifier[] {
        const locationSpecifiers: databaseLocationSpecifier[] = [];
        
        this.shards().forEach(shard => {
            shard.nodes().forEach(node => {
                locationSpecifiers.push({
                    nodeTag: node.tag,
                    shardNumber: shard.shardNumber
                })
            })
        })
        
        return locationSpecifiers;
    }
    
    toDto(): ShardedDatabaseInfo {
        return {
            ...super.toDto(),
            isSharded: true,
            shards: this.shards().map(x => x.toDto() as NonShardedDatabaseInfo)
        }
    }

    updateUsing(incomingCopy: StudioDatabaseInfo) {
        super.updateUsing(incomingCopy);

        const topology = incomingCopy.Sharding.Orchestrator.NodesTopology;

        const nodes = [
            ...topology.Members.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Promotables.map(x => this.mapNode(topology, x, "Promotable")),
            ...topology.Rehabs.map(x => this.mapNode(topology, x, "Rehab")),
        ];

        this.nodes(nodes);
        const nodeTag = this.clusterNodeTag();
        this.relevant(nodes.some(x => x.tag === nodeTag));

        const shards = Object.entries(incomingCopy.Sharding.Shards).map((kv) => {
            const [shardNumber, shardTopology] = kv;

            const shardNumberAsNumber = parseInt(shardNumber, 10);
            const s = new shard(incomingCopy, shardNumberAsNumber, shardTopology, this);
            s.deletionInProgress(shardedDatabase.extractDeletionInProgress(incomingCopy.DeletionInProgress, shardNumberAsNumber));
            
            return s;
        })
        this.shards(shards);
        this.relevant(nodes.some(x => x.tag === this.clusterNodeTag()));
        this.fixOrder(incomingCopy.Sharding.Orchestrator.NodesTopology.PriorityOrder.length > 0);
        
        // wipe out global deletion in progress - we store this info per shard
        this.deletionInProgress([]);
    }
    
    private static extractDeletionInProgress(rawDto: Record<string, DeletionInProgressStatus>, shardNumber: number): Array<{ tag: string, status: DeletionInProgressStatus}> {
        const shardStatus = Object.entries(rawDto).filter(x => x[0].endsWith("$" + shardNumber));
        
        return shardStatus.map(x => ({
            tag: x[0].split("$")[0],
            status: x[1]
        }));
    }
}

export = shardedDatabase;
