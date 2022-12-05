import React, { useCallback, useEffect, useReducer } from "react";
import { Alert, Button, FormGroup, Input, Label, Spinner } from "reactstrap";
import { UncontrolledButtonWithDropdownPanel } from "components/common/DropdownPanel";
import useId from "hooks/useId";
import useBoolean from "hooks/useBoolean";
import { useServices } from "hooks/useServices";
import { NodeInfoComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { manageDatabaseGroupReducer } from "components/pages/resources/manageDatabaseGroup/reducer";
import database from "models/resources/database";

// eslint-disable-next-line @typescript-eslint/no-empty-interface
interface ManageDatabaseGroupPageProps {
    db: database;
    //TODO:
}

export function ManageDatabaseGroupPage(props: ManageDatabaseGroupPageProps) {
    const { db } = props;

    //TODO: reorder nodes

    //TODO: permissions! (Requried role etc)

    const [state, dispatch] = useReducer(manageDatabaseGroupReducer, null); // TODO initial state?

    const { databasesService } = useServices();

    //TODO: error handling!
    const fetchDatabaseInfo = useCallback(
        async (databaseName: string) => {
            const info = await databasesService.getDatabase(databaseName);
            dispatch({
                type: "DatabaseInfoLoaded",
                info,
            });
        },
        [databasesService]
    );

    useEffect(() => {
        if (db) {
            fetchDatabaseInfo(db.name);
        }
    }, [fetchDatabaseInfo, db]);

    const settingsUniqueId = useId("settings");

    const { value: dynamicDatabaseDistribution, toggle: toggleDynamicDatabaseDistribution } = useBoolean(false);

    if (!state) {
        return <Spinner />;
    }

    return (
        <div className="content-margin">
            <div>
                <Button data-bind="click: enableNodesSort, enable: nodes().length > 1, requiredAccess: 'Operator'">
                    <i className="icon-reorder"></i> Reorder nodes
                </Button>
                <Button color="primary" data-bind="click: addNode, enable: addNodeEnabled, requiredAccess: 'Operator'">
                    <i className="icon-plus"></i>
                    <span>Add node to group</span>
                </Button>
                <UncontrolledButtonWithDropdownPanel buttonText="Settings">
                    <div>
                        <FormGroup switch className="form-check-reverse">
                            <Input
                                id={settingsUniqueId}
                                type="switch"
                                role="switch"
                                checked={dynamicDatabaseDistribution}
                                onChange={toggleDynamicDatabaseDistribution}
                            />
                            <Label htmlFor={settingsUniqueId} check>
                                Allow dynamic database distribution
                            </Label>

                            <Alert color="warning">HERE GOES dynamicDatabaseDistributionWarning</Alert>
                        </FormGroup>
                    </div>

                    {/*  TODO
                         <div class="settings-item padding padding-xs">
        <div class="flex-horizontal">
            <div class="control-label flex-grow">Allow dynamic database distribution</div>
            <div class="flex-noshrink">
                <div class="toggle">
                    <input type="checkbox" class="styled"
                           data-bind="checked: dynamicDatabaseDistribution, enable: enableDynamicDatabaseDistribution,
                                      requiredAccess: 'Operator', requiredAccessOptions: { strategy: 'disable' }">
                    <label></label>
                </div>
            </div>
        </div>
        <div class="help-block text-warning bg-warning" data-bind="visible: dynamicDatabaseDistributionWarning">
            <span data-bind="text: dynamicDatabaseDistributionWarning"></span>
        </div>
    </div>
                         */}
                </UncontrolledButtonWithDropdownPanel>
            </div>
            <div>
                {/* TODO MAP ALL KINDS */}
                {state.nodes.map((node) => (
                    <NodeInfoComponent key={node.tag} node={node} />
                ))}
            </div>
        </div>
    );
}

/* TODO
<div class="">
    <div class="row flex-row flex-grow flex-stretch-items">
        <div class="col-sm-12 col-lg-6 flex-vertical">
            <div class="flex-header flex-horizontal">
                <div data-bind="visible: inSortableMode">
                    Drag elements to set their order. Click 'Save' when finished.
                </div>
                <div class="flex-separator">
                </div>
                <div data-bind="visible: inSortableMode">
                    <button class="btn btn-primary" data-bind="click: saveNewOrder, enable: dirtyFlag().isDirty()">
                        <i class="icon-save"></i>
                        <span>Save</span>
                    </button>
                    <button class="btn btn-default" data-bind="click: cancelReorder">
                        <i class="icon-cancel"></i>
                        <span>Cancel</span>
                    </button>
                </div>
            </div>
            <div data-bind="visible: inSortableMode">
                <div class="flex-form">
                    <div class="form-group">
                        <label class="control-label">After failure recovery</label>
                        <div>
                            <div class="btn-group">
                                <button type="button" class="btn btn-default"
                                        data-bind="click: _.partial(fixOrder, false), css: { active: !fixOrder() }">
                                    Shuffle nodes order
                                </button>
                                <button type="button" class="btn btn-default"
                                        data-bind="click: _.partial(fixOrder, true), css: { active: fixOrder() }">
                                    Try to maintain nodes order
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="scroll flex-grow nodes-list" data-bind="css: { 'sort-mode' : inSortableMode }">
                <div class="not-deleted-nodes" data-bind="foreach: nodes">
                    <div class="panel destination-item panel-hover">
                        <div data-bind="attr: { 'data-state-text': badgeText,  class: 'state ' + badgeClass() +' nowrap' } "></div>
                        <div class="padding padding-sm destination-info">
                            <div class="info-container flex-horizontal flex-grow">
                                <div class="node flex-grow">
                                    <h5>NODE</h5>
                                    <h3 class="destination-name" data-bind="attr: { title: type() }">
                                        <i data-bind="attr: { class: cssIcon }"></i><span data-bind="text: 'Node ' + tag()"></span>
                                    </h3>
                                </div>
                                <div data-bind="visible: responsibleNode">
                                    <div class="text-center">
                                        <i class="icon-cluster-node" title="Database group node that is responsible for caught up of this node"></i>
                                        <span data-bind="text: responsibleNode" title="Database group node that is responsible for caught up of this node"></span>
                                    </div>
                                </div>
                                
                            </div>
                            <div class="actions-container" data-bind="visible: !$root.inSortableMode()">
                                <div class="actions">
                                   
                                    <div class="dropdown dropdown-right" data-bind="visible: $root.lockMode() === 'Unlock', requiredAccess: 'Operator'">
                                        <button class="btn btn-danger dropdown-toggle" type="button" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true">
                                            <i class="icon-disconnected"></i> <span>Delete from group</span>
                                            <span class="caret"></span>
                                        </button>
                                        <ul class="dropdown-menu">
                                            
                                        </ul>
                                    </div>
                                    <div class="has-disable-reason" data-original-title="Database cannot be deleted from node because of the set lock mode" 
                                         data-bind="visible: $root.lockMode() !== 'Unlock', requiredAccess: 'Operator'">
                                        <div class="btn btn-default disabled">
                                            <i class="icon-trash-cutout" 
                                               data-bind="css: { 'icon-addon-exclamation': $root.lockMode() === 'PreventDeletesError', 'icon-addon-cancel': $root.lockMode() === 'PreventDeletesIgnore' }"></i> 
                                            <span>Delete from group</span>
                                        </div>    
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="panel-addon" data-bind="visible: lastError">
                            <div class="padding small" data-bind="css: { 'bg-danger': badgeClass() === 'state-danger', 'bg-warning': badgeClass() === 'state-warning' }">
                                <div data-bind="text: lastErrorShort()"></div>
                                <div>
                                    <a href="#" data-bind="click: _.partial($root.showErrorDetails, tag())">show details</a>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="deleted-nodes" data-bind="foreach: deletionInProgress">
                    <div class="panel destination-item panel-hover">
                        <div data-state-text="Deleting" class="state state-warning"></div>
                        <div class="padding padding-sm destination-info">
                            <div class="info-container flex-horizontal flex-grow">
                                <div class="node flex-grow">
                                    <h5>NODE</h5>
                                    <h3 class="destination-name pulse text-warning" title="Deletion in progress">
                                        <i class="icon-trash"></i><span data-bind="text: 'Node ' + $data"></span>
                                    </h3>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>        
    </div>
</div>


 */

/* todo

class manageDatabaseGroup extends viewModelBase {

    dynamicDatabaseDistribution = ko.observable<boolean>(false);
    nodes = ko.observableArray<databaseGroupNode>([]);
    deletionInProgress = ko.observableArray<string>([]);
    isEncrypted = ko.observable<boolean>(false);
    
    lockMode = ko.observable<Raven.Client.ServerWide.DatabaseLockMode>();
    priorityOrder = ko.observableArray<string>([]);
    
    fixOrder = ko.observable<boolean>(false);
    
    clearNodesList = ko.observable<boolean>(false);
    
    selectedClusterNode = ko.observable<string>();
    
    inSortableMode = ko.observable<boolean>(false);
    private sortable: any;

    backupsOnly = false;

    nodeTag = clusterTopologyManager.default.localNodeTag;
    addNodeEnabled: KnockoutComputed<boolean>;
    
    enableDynamicDatabaseDistribution: KnockoutComputed<boolean>;
    dynamicDatabaseDistributionWarning: KnockoutComputed<string>;
    
    anyNodeHasError: KnockoutComputed<boolean>;

    private initObservables() {
        this.anyNodeHasError = ko.pureComputed(() => {
            if (clusterTopologyManager.default.votingInProgress()) {
                return true;
            }
            
            const topology = clusterTopologyManager.default.topology();
            
            if (!topology) {
                return true;
            }
            
            const nodes = topology.nodes();
            
            let allConnected = true;
            
            for (let i = 0; i < nodes.length; i++) {
                if (!nodes[i].connected()) {
                    allConnected = false;
                }
            }
            
            return !allConnected;
        });
        
        this.addNodeEnabled = ko.pureComputed(() => {
            const tags = clusterTopologyManager.default.topology().nodes().map(x => x.tag());
            const existingTags = this.nodes().map(x => x.tag());
            return _.without(tags, ...existingTags).length > 0;
        });

        this.enableDynamicDatabaseDistribution = ko.pureComputed(() => {
            return license.licenseStatus().HasDynamicNodesDistribution && !this.isEncrypted() && this.nodes().length > 1;
        });

        this.dynamicDatabaseDistributionWarning = ko.pureComputed(() => {
            if (!license.licenseStatus().HasDynamicNodesDistribution) {
                return "Your current license doesn't include the dynamic nodes distribution feature."
            }
            
            if (this.isEncrypted()) {
                return "Dynamic database distribution is not available when database is encrypted.";
            }
            
            if (this.nodes().length === 1) {
                return "There is only one node in the group.";
            }
            
            return null;
        });
        
        this.registerDisposable(
            this.anyNodeHasError.subscribe((error) => {
                if (error && this.inSortableMode()) {
                    messagePublisher.reportWarning("Can't reorder nodes, when at least one node is down or voting is in progress.");
                    this.cancelReorder();
                }
            }));
    }

    activate(args: any) {
        super.activate(args);
        
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }
    
    attached() {
        super.attached();

        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchAllDatabaseChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));
    }

        this.dynamicDatabaseDistribution.subscribe(dynamic => {
            new toggleDynamicNodeAssignmentCommand(this.activeDatabase().name, dynamic)
                .execute();
        });
    
  
    enableNodesSort() {
        this.inSortableMode(true);

        const list = $(".nodes-list .not-deleted-nodes")[0];

        this.sortable = new Sortable(list as HTMLElement,
            {
                onEnd: (event: { oldIndex: number, newIndex: number }) => {
                    const nodes = this.nodes();
                    nodes.splice(event.newIndex, 0, nodes.splice(event.oldIndex, 1)[0]);
                    this.nodes(nodes);
                }
            });
        
        this.dirtyFlag().reset();
    }

    cancelReorder() {
        this.disableNodesSort();
    }

    saveNewOrder() {
        eventsCollector.default.reportEvent("db-group", "save-order");
        const newOrder = this.nodes().map(x => x.tag());
        
        new reorderNodesInDatabaseGroupCommand(this.activeDatabase().name, newOrder, this.fixOrder())
            .execute()
            .done(() => {
                this.disableNodesSort();
                this.dirtyFlag().reset();
            });
    }
    
    private disableNodesSort() {
        this.inSortableMode(false);
        
        if (this.sortable) {
            this.sortable.destroy();
            this.sortable = null;
        }

        // hack: force list to be empty - sortable (RubaXa version) doesn't play well with ko:foreach
        // https://github.com/RubaXa/Sortable/issues/533
        this.clearNodesList(true);
        
        // fetch fresh copy
        this.refresh();
    }
    
    private refresh() {
        if (!this.inSortableMode()) {
            this.fetchDatabaseInfo()
        }
    }
    
    private fetchDatabaseInfo() {
        return new getDatabaseCommand(this.activeDatabase().name)
            .execute()
            .done(dbInfo => {
                this.onDatabaseInfoFetched(dbInfo);
            });
    }

  
    private onDatabaseInfoFetched(dbInfoDto: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        const incomingDbInfo = new databaseInfo(dbInfoDto);
        
        if (this.clearNodesList()) {
            $(".nodes-list .not-deleted-nodes").empty();
            this.nodes([]);
            this.clearNodesList(false);
        }
        
        this.updateNodes(incomingDbInfo.nodes());
        this.deletionInProgress(incomingDbInfo.deletionInProgress());
        this.isEncrypted(incomingDbInfo.isEncrypted());
        this.dynamicDatabaseDistribution(incomingDbInfo.dynamicDatabaseDistribution());
        this.priorityOrder(incomingDbInfo.priorityOrder());
        this.lockMode(incomingDbInfo.lockMode());
        this.fixOrder(incomingDbInfo.priorityOrder() && incomingDbInfo.priorityOrder().length > 0);
    }
    
    private updateNodes(incomingData: databaseGroupNode[]) {
        const local = this.nodes();
        
        const localTags = local.map(x => x.tag());
        const remoteTags = incomingData.map(x => x.tag());
        
        if (_.isEqual(localTags, remoteTags)) {
            // we have same node tags: do in place update
         
            incomingData.forEach(d => {
                local.find(x => x.tag() === d.tag()).update(d);
            });
        } else {
            // node tags changed
            this.nodes(incomingData);
        }
    }

    addNode() {
        const addKeyView = new addNewNodeToDatabaseGroup(this.activeDatabase().name, this.nodes(), this.isEncrypted());
        app.showBootstrapDialog(addKeyView);
    }
    
    deleteNodeFromGroup(node: databaseGroupNode, hardDelete: boolean) {
        const db = this.activeDatabase();
        const nodeTag = node.tag();
        this.confirmationMessage("Are you sure", "Do you want to delete database '" + generalUtils.escapeHtml(this.activeDatabase().name) + "' from node: " + node.tag() + "?", {
            buttons: ["Cancel", "Yes, delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    new deleteDatabaseFromNodeCommand(db, [nodeTag], hardDelete)
                        .execute();
                }
            });
    }

    showErrorDetails(tag: string) {
        const node = this.nodes().find(x => x.tag() === tag);

        app.showBootstrapDialog(new showDataDialog("Error details. Node: " + tag, node.lastError(), "plain"));
    }

}

export = manageDatabaseGroup;

 */
