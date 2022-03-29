import shardViewModelBase from "viewmodels/shardViewModelBase";
import { IndexesPage } from "../../../components/pages/database/indexes/IndexesPage";

export class indexes extends shardViewModelBase {
    view = { default: `<div class="indexes content-margin no-transition absolute-fill" data-bind="react: reactOptions"></div>` };
    
    props: Parameters<typeof IndexesPage>[0];
    
    activate(args: any, parameters?: any) {
        super.activate(args, parameters);
        
        this.props = {
            database: this.db,
            indexToHighlight: args?.indexName,
            stale: args?.stale || false
        }
    }

    reactOptions = ko.pureComputed(() => ({
        component: IndexesPage,
        props: this.props
    }));
}
