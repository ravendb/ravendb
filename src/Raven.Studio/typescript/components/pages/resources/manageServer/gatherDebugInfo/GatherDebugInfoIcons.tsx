import { Icon } from "components/common/Icon";
import React from "react";
import IconName from "typings/server/icons";

function GatherDebugInfoIcons() {
    const icons: IconName[] = ["replication", "stats", "io-test", "storage", "memory", "other"];
    const labels = ["Replication", "Performance", "I/O", "Storage", "Memory", "Other"];
    return (
        <div className="d-flex flex-row my-3 gap-4 flex-wrap justify-content-center icons-list">
            {icons.map((icon, index) => (
                <div key={icon} className="d-flex flex-column align-items-center text-center gap-3">
                    <Icon icon={icon} margin="m-0" />
                    <p>{labels[index]}</p>
                </div>
            ))}
        </div>
    );
}

export default GatherDebugInfoIcons;
