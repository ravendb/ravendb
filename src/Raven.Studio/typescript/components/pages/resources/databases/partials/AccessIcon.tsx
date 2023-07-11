import { Icon } from "components/common/Icon";
import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";

export function AccessIcon(props: { dbAccess: databaseAccessLevel }) {
    const { dbAccess } = props;
    switch (dbAccess) {
        case "DatabaseAdmin":
            return <Icon icon="access-admin" title="Admin Access" />;
        case "DatabaseReadWrite":
            return <Icon icon="access-read-write" title="Read/Write Access" />;
        case "DatabaseRead":
            return <Icon icon="access-read" title="Read-only Access" />;
        default:
            assertUnreachable(dbAccess);
    }
}
