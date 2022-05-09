import viewModelBase = require("viewmodels/viewModelBase");
import activator = require("durandal/activator");
import database = require("models/resources/database");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import shardingContext from "viewmodels/common/sharding/shardingContext";
import router = require("plugins/router");

class shardAwareContainer extends viewModelBase {
    protected rootActivator: DurandalActivator<any>;
    private readonly childCtr: new (db: database, location: databaseLocationSpecifier, state?: any) => shardViewModelBase;
    
    context: shardingContext;
    usingExternalContext: boolean;

    activationData: any;

    child = ko.observable<shardViewModelBase>();
    view = require("views/common/sharding/shardAwareContainer.html");
    
    constructor(mode: shardingMode, 
                childCtr: new (db: database, location: databaseLocationSpecifier, state?: any) => shardViewModelBase, 
                externalContext?: shardingContext) {
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
        
        this.context.onChange((db, location) => {
            if (db) {
                this.useDatabase(db, location);
            }
        });
        
        this.context.resetView();
    }

    useDatabase(db: database, location: databaseLocationSpecifier) {
        const oldChild = this.child();
        const oldViewState = oldChild?.getViewState?.();

        this.activateChildView(db, location, oldViewState);
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
    
    private activateChildView(db: database, location: databaseLocationSpecifier, state?: any) {
        const child = new this.childCtr(db, location, state);
        
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
