import database from "models/resources/database";
import React from "react";
import shardedReactViewModelBase from "viewmodels/shardedReactViewModelBase";
import reactViewModelBase from "viewmodels/reactViewModelBase";
import assertUnreachable from "components/utils/assertUnreachable";

type viewType = 
    "shardedView" // use when parent of source view was: shardViewModelBase 
    | "nonShardedView"; // use when parent of source view was: viewModelBase

export function bridgeToReact(reactView: React.FC<any>, viewType: viewType, bootstrap5 = true) {
    switch (viewType) {
        case "shardedView":
            return class extends shardedReactViewModelBase {
                constructor(db: database, location: databaseLocationSpecifier) {
                    super(db, location, reactView, bootstrap5);
                }
            }
        case "nonShardedView":
            return class extends reactViewModelBase {
                constructor() {
                    super(reactView, bootstrap5);
                }
            }
        default:
            assertUnreachable(viewType);
    }
}

// ReactToKnockoutComponent example of use:
// class MyList extends ReactToKnockoutComponent<{ list: number[] }> {
//     ref = createRef<HTMLDivElement>();
//
//     render() {
//         return (
//             <div ref={this.ref}>
//                 <ul data-bind="foreach: props.list">
//                     <li data-bind="text: $data"></li>
//                 </ul>
//             </div>
//         );
//     }
// }

export class ReactToKnockoutComponent<P = any, S = any> extends React.PureComponent<P, S> {
    [x: string]: any;

    updateKnockout() {
        this.__koTrigger(!this.__koTrigger());
    }

    componentDidMount() {
        this.__koTrigger = ko.observable(true);
        this.__koModel = ko.computed(function () {
            this.__koTrigger();

            return {
                props: this.props,
                state: this.state,
            };
        }, this);
        ko.applyBindings(this.__koModel, this.ref.current);
    }

    componentWillUnmount() {
        ko.cleanNode(this.ref.current);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    componentDidUpdate(prevProps: Readonly<P>, prevState: Readonly<S>) {
        this.updateKnockout();
    }
}
