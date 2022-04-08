import viewModelBase = require("viewmodels/viewModelBase");
import activator = require("durandal/activator");
import database = require("models/resources/database");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import shardingContext from "viewmodels/common/sharding/shardingContext";
import { shardingTodo } from "common/developmentHelper";
import router = require("plugins/router");

class shardAwareContainer extends viewModelBase {
    protected rootActivator: DurandalActivator<any>;
    private readonly childCtr: new (db: database, state?: any) => shardViewModelBase;
    
    context: shardingContext;
    usingExternalContext: boolean;

    activationData: any;

    child = ko.observable<shardViewModelBase>();
    view = require("views/common/sharding/shardAwareContainer.html");
    
    constructor(mode: shardingMode, childCtr: new (db: database, state?: any) => shardViewModelBase, externalContext?: shardingContext) {
        super();
        
        this.context = externalContext ?? new shardingContext(mode);
        this.usingExternalContext = !!externalContext;
        this.rootActivator = activator.create();
        this.childCtr = childCtr;
    }
    
    activate(args: any, parameters?: any) {
        super.activate(args, parameters);
        
        this.activationData = args;
    }

    compositionComplete() {
        super.compositionComplete();
        
        this.context.onChange(db => {
            if (db) {
                this.useDatabase(db);
            } else {
                //TODO: ?
            }
        });
        
        this.context.resetView();
    }

    useDatabase(db: database) {
        shardingTodo("Marcin");
        // TODO: allow to persist between views - check if local / remote
        //TODO: doesn't work when pinning
        const oldChild = this.child();
        const oldViewState = oldChild?.getViewState?.();

        this.activateChildView(db); //TODO: old child state
    }

    canDeactivate(isClose: boolean): boolean | JQueryPromise<canDeactivateResultDto> {
        return this.rootActivator.canDeactivate(isClose)
            .then(result => ({ can: result }))
    }

    deactivate() {
        super.deactivate();

        if (this.child()) {
            this.child(null);
            return this.rootActivator.deactivate(true);
        }
    }
    
    private activateChildView(db: database) {
        const child = new this.childCtr(db);
        
        this.rootActivator.activateItem(child, this.activationData)
            .done((result) => {
                if (result) {
                    this.child(child);
                } else {
                    const lifecycleData = (this.rootActivator.settings as any).lifecycleData;
                    if (lifecycleData && lifecycleData.redirect) {
                        router.navigate(lifecycleData.redirect);
                    }
                }
            });
    }
}

export = shardAwareContainer;
