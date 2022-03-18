import shardViewModelBase from "viewmodels/shardViewModelBase";
import { IndexesPage } from "../../../components/pages/database/indexes/IndexesPage";

class indexes extends shardViewModelBase {
    view = { default: `<div class="indexes content-margin no-transition absolute-fill" data-bind="react: reactOptions"></div>` };
    
    reactOptions = ko.pureComputed(() => ({
        component: IndexesPage,
        props: {
            database: this.db
        }
    }));
}

export = indexes;
