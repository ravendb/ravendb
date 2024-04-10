import { Icon } from "components/common/Icon";
import { StudioSearchResultDatabaseGroup } from "components/shell/studioSearchWithDatabaseSelector/studioSearch/studioSearchTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";

interface StudioSearchDatabaseGroupHeaderProps {
    groupType: StudioSearchResultDatabaseGroup;
}

export default function StudioSearchDatabaseGroupHeader({ groupType }: StudioSearchDatabaseGroupHeaderProps) {
    return (
        <strong className="text-uppercase">
            <GroupHeaderBody groupType={groupType} />
        </strong>
    );
}

interface GroupHeaderBodyProps {
    groupType: StudioSearchResultDatabaseGroup;
}

function GroupHeaderBody({ groupType }: GroupHeaderBodyProps) {
    switch (groupType) {
        case "collections":
            return (
                <>
                    <Icon icon="documents" style={{ color: "#2f9ef3" }} />
                    Collections
                </>
            );
        case "documents":
            return (
                <>
                    <Icon icon="document" style={{ color: "#2f9ef3" }} />
                    Documents
                </>
            );
        case "indexes":
            return (
                <>
                    <Icon icon="index" style={{ color: "#945ab5" }} />
                    Indexes
                </>
            );
        case "tasks":
            return (
                <>
                    <Icon icon="tasks" style={{ color: "#f06582" }} />
                    Tasks
                </>
            );
        case "settings":
            return (
                <>
                    <Icon icon="settings" style={{ color: "#f0b362" }} />
                    Settings
                </>
            );
        case "stats":
            return (
                <>
                    <Icon icon="stats" style={{ color: "#7bd85d" }} />
                    Stats
                </>
            );
        default:
            assertUnreachable(groupType);
    }
}
