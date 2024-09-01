/// <reference path="../../../typings/tsd.d.ts"/>

import DeletionInProgressStatus = Raven.Client.ServerWide.DeletionInProgressStatus;
import accessManager from "common/shell/accessManager";
import { DatabaseSharedInfo, NodeInfo } from "components/models/databases";
import NodeId = Raven.Client.ServerWide.Operations.NodeId;
import NodesTopology = Raven.Client.ServerWide.Operations.NodesTopology;
import StudioDatabaseInfo = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabases.StudioDatabaseInfo;
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import type shardedDatabase from "models/resources/shardedDatabase";

abstract class database {
    static readonly type = "database";
    static readonly qualifier = "db";

    name: string;

    disabled = ko.observable<boolean>(false);
    errored = ko.observable<boolean>(false);
    relevant = ko.observable<boolean>(true);
    nodes = ko.observableArray<NodeInfo>([]);
    hasRevisionsConfiguration = ko.observable<boolean>(false);
    hasExpirationConfiguration = ko.observable<boolean>(false);
    hasRefreshConfiguration = ko.observable<boolean>(false);
    hasArchivalConfiguration = ko.observable<boolean>(false);
    isEncrypted = ko.observable<boolean>(false);
    lockMode = ko.observable<DatabaseLockMode>();
    deletionInProgress = ko.observableArray<{ tag: string, status: DeletionInProgressStatus }>([]);
    dynamicNodesDistribution = ko.observable<boolean>(false);
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();

    databaseAccess = ko.observable<databaseAccessLevel>();
    databaseAccessText = ko.observable<string>();
    databaseAccessColor = ko.observable<string>();
    
    fixOrder = ko.observable<boolean>(false);
    
    indexesCount = ko.observable<number>();
    
    clusterNodeTag: KnockoutObservable<string>;
    
    abstract get root(): database;
    
    abstract isSharded(): this is shardedDatabase;

    abstract getLocations(): databaseLocationSpecifier[];

    /**
     * Gets first location - but prefers local node tag
     * @param preferredNodeTag
     */
    getFirstLocation(preferredNodeTag: string): databaseLocationSpecifier {
        const preferredMatch = this.getLocations().find(x => x.nodeTag === preferredNodeTag);
        if (preferredMatch) {
            return preferredMatch;
        }
        
        return this.getLocations()[0];
    }
    
    protected constructor(dbInfo: StudioDatabaseInfo, clusterNodeTag: KnockoutObservable<string>) {
        this.clusterNodeTag = clusterNodeTag;
    }
    
    static createEnvironmentColorComputed(prefix: string, source: KnockoutObservable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>) {
        return ko.pureComputed(() => {
            const env = source();
            if (env) {
                switch (env) {
                    case "Production":
                        return prefix + "-danger";
                    case "Testing":
                        return prefix + "-success";
                    case "Development":
                        return prefix + "-info";
                }
            }

            return null;
        });
    }

    updateUsing(incomingCopy: StudioDatabaseInfo) {
        this.isEncrypted(incomingCopy.IsEncrypted);
        this.name = incomingCopy.Name;
        this.disabled(incomingCopy.IsDisabled);
        this.lockMode(incomingCopy.LockMode);
        
        this.indexesCount(incomingCopy.IndexesCount);
        
        this.deletionInProgress(Object.entries(incomingCopy.DeletionInProgress).map((kv: [string, DeletionInProgressStatus]) => {
            return {
                tag: kv[0],
                status: kv[1]
            }
        }));
        
        this.hasRevisionsConfiguration(incomingCopy.HasRevisionsConfiguration);
        this.hasExpirationConfiguration(incomingCopy.HasExpirationConfiguration);
        this.hasRefreshConfiguration(incomingCopy.HasRefreshConfiguration);
        this.hasArchivalConfiguration(incomingCopy.HasDataArchivalConfiguration);
        
        this.environment(incomingCopy.StudioEnvironment !== "None" ? incomingCopy.StudioEnvironment : null);
        
        //TODO: delete
        const dbAccessLevel = accessManager.default.getEffectiveDatabaseAccessLevel(incomingCopy.Name);
        this.databaseAccess(dbAccessLevel);
        this.databaseAccessText(accessManager.default.getAccessLevelText(dbAccessLevel));
        this.databaseAccessColor(accessManager.default.getAccessColor(dbAccessLevel));
    }
    
    isBeingDeleted() {
        const localTag = this.clusterNodeTag();
        return this.deletionInProgress().some(x => x.tag === localTag);
    }

    static getNameFromUrl(url: string) {
        const index = url.indexOf("databases/");
        return (index > 0) ? url.substring(index + 10) : "";
    }

    toDto(): DatabaseSharedInfo {
        return {
            name: this.name,
            isEncrypted: this.isEncrypted(),
            isSharded: false,
            nodes: this.nodes(),
            isDisabled: this.disabled(),
            currentNode: { 
                isRelevant: this.relevant(),
                isBeingDeleted: this.isBeingDeleted()
            },
            isDynamicNodesDistribution: this.dynamicNodesDistribution(),
            isFixOrder: this.fixOrder(),
            indexesCount: this.indexesCount(),
            lockMode: this.lockMode(),
            deletionInProgress: this.deletionInProgress().map(x => x.tag),
            environment: this.environment(),
        }
    }
    
    //TODO: remove those props?
    get fullTypeName() {
        return "Database";
    }

    get qualifier() {
        return database.qualifier;
    }


    get type() {
        return database.type;
    }

    protected mapNode(topology: NodesTopology, node: NodeId, type: databaseGroupNodeType): NodeInfo {
        return {
            tag: node.NodeTag,
            nodeUrl: node.NodeUrl,
            type,
            responsibleNode: node.ResponsibleNode,
            lastError: topology.Status?.[node.NodeTag]?.LastError,
            lastStatus: topology.Status?.[node.NodeTag]?.LastStatus,
        }
    }
}

export = database;
