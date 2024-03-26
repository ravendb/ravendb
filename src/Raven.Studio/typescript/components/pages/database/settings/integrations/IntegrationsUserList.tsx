import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import IntegrationsUserListItem from "components/pages/database/settings/integrations/IntegrationsUserListItem";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";

interface IntegrationsUserListProps {
    fetchState: AsyncStateStatus;
    reload: () => void;
    users: string[];
    removeUser: (index: number) => void;
}

export default function IntegrationsUserList(props: IntegrationsUserListProps) {
    const { fetchState, reload, users, removeUser } = props;

    if (fetchState === "loading") {
        return <LoadingView />;
    }

    if (fetchState === "error") {
        return <LoadError error="TOOD" refresh={reload} />;
    }

    if (users.length === 0) {
        return <EmptySet>No credentials</EmptySet>;
    }

    return users.map((user, idx) => (
        <IntegrationsUserListItem key={idx} initialUsername={user} removeUser={() => removeUser(idx)} />
    ));
}
