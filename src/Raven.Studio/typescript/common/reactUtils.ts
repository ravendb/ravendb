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
