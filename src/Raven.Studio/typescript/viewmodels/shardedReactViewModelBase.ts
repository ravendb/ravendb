import shardViewModelBase from "viewmodels/shardViewModelBase";
import React from "react";
import database from "models/resources/database";
import { getReactDirtyFlag } from "common/reactViewModelUtils";

abstract class shardedReactViewModelBase extends shardViewModelBase {

    view = { default: `<div class="react-container" data-bind="react: reactOptions"></div>` };

    private readonly reactView: React.FC<any>;
    private readonly bootstrap5: boolean; //TODO: will be removed once we migrate all react views to bs5 (I assume one left)

    protected constructor(db: database, location: databaseLocationSpecifier, reactView: React.FC<any>, bootstrap5 = true) {
        super(db, location);

        this.reactView = reactView;
        this.bootstrap5 = bootstrap5;
    }

    isUsingBootstrap5() {
        return this.bootstrap5;
    }

    reactOptions: ReactInKnockout<any>;

    activate(args: any, parameters?: any) {
        super.activate(args, parameters);

        const reactDirtyFlag = getReactDirtyFlag(this.dirtyFlag);
        const reactProps = {
            ...args,
            database: this.db,
            location: this.location,
        };

        this.reactOptions = this.createReactOptions(this.reactView, reactProps, reactDirtyFlag);
    }

    createReactOptions<TProps = unknown>(component: (props?: TProps) => JSX.Element, props?: TProps, dirtyFlag?: ReactDirtyFlag) {
        return ko.pureComputed(() => ({
            component,
            props,
            dirtyFlag
        }));
    }
}


export = shardedReactViewModelBase;
