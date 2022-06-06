import shardViewModelBase from "viewmodels/shardViewModelBase";
import { OngoingTasksPage } from "../../../components/pages/database/tasks/list/OngoingTasksPage";

class ongoingTasks extends shardViewModelBase {
    view = { default: `<section class="destinations flex-vertical absolute-fill content-margin manage-ongoing-tasks"
 data-bind="react: reactOptions"></section>` };

    props: Parameters<typeof OngoingTasksPage>[0];

    activate(args: any, parameters?: any) {
        super.activate(args, parameters);

        this.props = {
            database: this.db,
        }
    }

    reactOptions = ko.pureComputed(() => ({
        component: OngoingTasksPage,
        props: this.props
    }));
}

export = ongoingTasks;
