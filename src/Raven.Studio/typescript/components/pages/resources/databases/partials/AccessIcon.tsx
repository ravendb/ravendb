import { Icon } from "components/common/Icon";
import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";

export function AccessIcon(props: { dbAccess: databaseAccessLevel }) {
    const { dbAccess } = props;
    switch (dbAccess) {
        case "DatabaseAdmin":
            return <Icon icon="access-admin" title="Admin Access" margin="m-0" />;
        case "DatabaseReadWrite":
            return <Icon icon="access-read-write" title="Read/Write Access" margin="m-0" />;
        case "DatabaseRead":
            return <Icon icon="access-read" title="Read-only Access" margin="m-0" />;
        default:
            assertUnreachable(dbAccess);
    }
}
