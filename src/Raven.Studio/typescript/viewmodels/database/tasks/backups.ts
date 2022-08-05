import shardViewModelBase from "viewmodels/shardViewModelBase";
import { BackupsPage } from "../../../components/pages/database/tasks/backups/BackupsPage";

class backups extends shardViewModelBase {
    view = { default: `<section class="flex-vertical absolute-fill content-margin backups"
 data-bind="react: reactOptions"></section>` };

    props: Parameters<typeof BackupsPage>[0];

    activate(args: any, parameters?: any) {
        super.activate(args, parameters);

        this.props = {
            database: this.db,
        }
    }

    reactOptions = ko.pureComputed(() => ({
        component: BackupsPage,
        props: this.props
    }));
}

export = backups;
