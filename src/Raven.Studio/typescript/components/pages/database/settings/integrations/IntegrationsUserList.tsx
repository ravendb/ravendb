import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import IntegrationsUserListItem from "components/pages/database/settings/integrations/IntegrationsUserListItem";
import { IntegrationsUser } from "components/pages/database/settings/integrations/useIntegrations";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";

interface IntegrationsUserListProps {
    fetchState: AsyncStateStatus;
    reload: () => void;
    users: IntegrationsUser[];
    removeUser: (index: number) => void;
}

export default function IntegrationsUserList(props: IntegrationsUserListProps) {
    const { fetchState, reload, users, removeUser } = props;

    if (fetchState === "loading") {
        return <LoadingView />;
    }

    if (fetchState === "error") {
        return <LoadError error="Unable to load credentials" refresh={reload} />;
    }

    if (users.length === 0) {
        return <EmptySet>No credentials have been defined</EmptySet>;
    }

    return users.map((user, idx) => (
        <IntegrationsUserListItem key={user.id} initialUsername={user.username} removeUser={() => removeUser(idx)} />
    ));
}
