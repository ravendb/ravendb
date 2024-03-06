import React from "react";
import { Label } from "reactstrap";
import { ConnectionStringUsedTask } from "../../connectionStringsTypes";
import { Icon } from "components/common/Icon";
import { CounterBadge } from "components/common/CounterBadge";

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
            <Label className="d-flex align-items-center gap-1">
                Used in tasks <CounterBadge count={tasks.length} />
            </Label>
            <div className="d-flex flex-wrap gap-2">
                {tasks.map((task) => (
                    <a
                        key={task.id}
                        href={urlProvider(task.id)()}
                        className="btn btn-primary rounded-pill"
                        title={task.name}
                    >
                        <Icon icon="ongoing-tasks" />
                        {task.name}
                    </a>
                ))}
            </div>
        </div>
    );
}
