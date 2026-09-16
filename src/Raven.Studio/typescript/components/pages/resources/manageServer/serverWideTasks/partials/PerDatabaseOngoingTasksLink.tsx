import React from "react";
import classNames from "classnames";
import appUrl from "common/appUrl";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export function PerDatabaseOngoingTasksLink() {
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <ConditionalPopover
            conditions={{
                isActive: !activeDatabaseName,
                message: "Select a database to go to its per-database ongoing tasks view",
            }}
        >
            <a
                className={classNames("btn btn-link btn-sm", { disabled: !activeDatabaseName })}
                href={activeDatabaseName ? appUrl.forOngoingTasks(activeDatabaseName) : undefined}
                title="Go to the Ongoing Tasks view of the selected database"
            >
                <Icon icon="ongoing-tasks" />
                Per-database ongoing tasks
            </a>
        </ConditionalPopover>
    );
}
