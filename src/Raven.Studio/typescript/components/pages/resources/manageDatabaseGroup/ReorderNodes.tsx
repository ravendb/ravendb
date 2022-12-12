import React from "react";

export function ReorderNodes() {
    //TODO review data-bind
    return <h1>TODO</h1>;
}

/* TODO
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
 <div class="flex-header flex-horizontal">
                <div data-bind="visible: inSortableMode">
                    Drag elements to set their order. Click 'Save' when finished.
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
 */
