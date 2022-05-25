import shardViewModelBase from "viewmodels/shardViewModelBase";
import { StatisticsPage } from "../../../components/pages/database/status/statistics/StatisticsPage";

class statistics extends shardViewModelBase {
    view = { default: `<section class="stats content-margin" data-bind="react: reactOptions"></section>` };

    props: Parameters<typeof StatisticsPage>[0];

    activate(args: any, parameters?: any) {
        super.activate(args, parameters);

        this.props = {
            database: this.db,
        }
    }
    
    reactOptions = ko.pureComputed(() => ({
        component: StatisticsPage,
        props: this.props
    }));
}

export = statistics;
