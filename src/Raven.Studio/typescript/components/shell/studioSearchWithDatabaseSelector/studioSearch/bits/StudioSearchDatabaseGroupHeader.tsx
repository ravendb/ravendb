import { Icon } from "components/common/Icon";
import { StudioSearchResultDatabaseGroup } from "components/shell/studioSearchWithDatabaseSelector/studioSearch/studioSearchTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";

interface StudioSearchDatabaseGroupHeaderProps {
    groupType: StudioSearchResultDatabaseGroup;
}

export default function StudioSearchDatabaseGroupHeader({ groupType }: StudioSearchDatabaseGroupHeaderProps) {
    return <GroupHeaderBody groupType={groupType} />;
}

interface GroupHeaderBodyProps {
    groupType: StudioSearchResultDatabaseGroup;
}

function GroupHeaderBody({ groupType }: GroupHeaderBodyProps) {
    switch (groupType) {
        case "collections":
            return (
                <>
                    <Icon icon="documents" />
                    Collections
                </>
            );
        case "documents":
            return (
                <>
                    <Icon icon="document" />
                    Documents
                </>
            );
        case "indexes":
            return (
                <>
                    <Icon icon="index" />
                    Indexes
                </>
            );
        case "tasks":
            return (
                <>
                    <Icon icon="tasks" />
                    Tasks
                </>
            );
        case "settings":
            return (
                <>
                    <Icon icon="settings" />
                    Settings
                </>
            );
        case "stats":
            return (
                <>
                    <Icon icon="stats" />
                    Stats
                </>
            );
        default:
            assertUnreachable(groupType);
    }
}
