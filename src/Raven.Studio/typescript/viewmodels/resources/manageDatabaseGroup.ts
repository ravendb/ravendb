import viewModelBase = require("viewmodels/viewModelBase");

import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import databaseInfo = require("models/resources/info/databaseInfo");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import clusterNode = require("models/database/cluster/clusterNode");
import addNodeToDatabaseGroupCommand = require("commands/database/dbGroup/addNodeToDatabaseGroupCommand");

class manageDatabaseGroup extends viewModelBase {

    currentDatabaseInfo = ko.observable<databaseInfo>();
    selectedClusterNode = ko.observable<string>();

    nodeTag = clusterTopologyManager.default.nodeTag;
    nodes: KnockoutComputed<clusterNode[]>;
    additionalNodes: KnockoutComputed<string[]>;
    addNodeEnabled: KnockoutComputed<boolean>;

    spinners = {
        addNode: ko.observable<boolean>(false)
    }

    constructor() {
        super();

        this.bindToCurrentInstance("addNode", "deleteNodeFromGroup");

        this.initObservables();
    }

    private initObservables() {
        this.nodes = ko.pureComputed(() => {
            const dbInfo = this.currentDatabaseInfo();
            return dbInfo.nodes();
        });

        this.additionalNodes = ko.pureComputed<string[]>(() => {
            const tags = clusterTopologyManager.default.topology().nodes().map(x => x.tag());
            const existingTags = this.nodes().map(x => x.tag());
            return _.without(tags, ...existingTags);
        });

        this.addNodeEnabled = ko.pureComputed(() => {
            const nodeSelected = !!this.selectedClusterNode();
            const inProgress = this.spinners.addNode();
            return nodeSelected && !inProgress;
        });
    }

    activate(args: any) {
        super.activate(args);

        return this.fetchDatabaseInfo();
    }

    private fetchDatabaseInfo() {
        return new getDatabaseCommand(this.activeDatabase().name)
            .execute()
            .done(dbInfo => this.onDatabaseInfoFetched(dbInfo));
    }

    private onDatabaseInfoFetched(dbInfoDto: Raven.Client.Server.Operations.DatabaseInfo) {
        const dbInfo = new databaseInfo(dbInfoDto);
        this.currentDatabaseInfo(dbInfo);
    }

    //TODO: remove in future - use live view instead
    refresh() {
        this.fetchDatabaseInfo();
    }

    addNode() {
        //TODO: handle case when encryption is enabled on this db

        this.spinners.addNode(true);

        new addNodeToDatabaseGroupCommand(this.activeDatabase().name, this.selectedClusterNode())
            .execute()
            .done(() => {
                this.selectedClusterNode(null);

                //TODO: delete me and simply use live view
                setTimeout(() => this.refresh(), 300);
            })
            .always(() => this.spinners.addNode(false));
    }

    deleteNodeFromGroup(node: clusterNode, hardDelete: boolean) {
        //TODO: implemenent me!
        console.log("deleting: " + node.tag());

        //TODO: refresh after deletion - auto update this in the future 
    }

    
}

export = manageDatabaseGroup;