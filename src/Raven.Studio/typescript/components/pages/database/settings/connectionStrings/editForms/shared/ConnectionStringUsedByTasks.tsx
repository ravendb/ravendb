import React from "react";
import { Label } from "reactstrap";
import { ConnectionStringUsedTask } from "../../connectionStringsTypes";
import { Icon } from "components/common/Icon";

interface ConnectionStringUsedByTasks {
    tasks: ConnectionStringUsedTask[];
    urlProvider: (taskId: number) => () => string;
}

export default function ConnectionStringUsedByTasks({ tasks, urlProvider }: ConnectionStringUsedByTasks) {
    if (!tasks || tasks.length === 0) {
        return null;
    }

    return (
        <div className="mt-2">
            <Label className="md-label">Used in Tasks</Label>
            <div className="d-flex flex-wrap gap-2">
                {tasks.map((task) => (
                    <a key={task.id} href={urlProvider(task.id)()} className="btn btn-primary rounded-pill">
                        <Icon icon="ongoing-tasks" />
                        {task.name}
                    </a>
                ))}
            </div>
        </div>
    );
}
