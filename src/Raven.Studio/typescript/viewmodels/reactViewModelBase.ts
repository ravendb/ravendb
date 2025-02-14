import React = require("react");
import viewModelBase = require("viewmodels/viewModelBase");
import reactViewModelUtils = require("common/reactViewModelUtils");
import router = require("plugins/router");

abstract class reactViewModelBase extends viewModelBase {

    view = { default: `<div class="react-container" data-bind="react: reactOptions"></div>` };

    private readonly reactView: React.FC<any>;
    private readonly bootstrap5: boolean; //TODO: will be removed once we migrate all react views to bs5 (I assume one left)

    protected constructor(reactView: React.FC<any>, bootstrap5 = true) {
        super();

        this.reactView = reactView;
        this.bootstrap5 = bootstrap5;
    }

    isUsingBootstrap5() {
        return this.bootstrap5;
    }

    reactOptions: ReactInKnockout<any>;

    activate(args: any, parameters?: any) {
        super.activate(args, parameters);
        const { params: pathParams, queryParams } = router.activeInstruction()

        const reactDirtyFlag = reactViewModelUtils.getReactDirtyFlag(this.dirtyFlag, this.customDiscardStayResult);
        const reactProps: ReactProps = {
          pathParams,
          queryParams: queryParams || {},
        };

        this.reactOptions = this.createReactOptions(this.reactView, reactProps, reactDirtyFlag);
    }

    createReactOptions<TProps = unknown>(component: React.FC<TProps>, props?: TProps, dirtyFlag?: ReactDirtyFlag) {
        return ko.pureComputed(() => ({
            component,
            props,
            dirtyFlag
        }));
    }
}

export = reactViewModelBase;
