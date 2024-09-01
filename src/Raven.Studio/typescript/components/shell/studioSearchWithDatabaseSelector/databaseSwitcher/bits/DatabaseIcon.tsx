import { Icon } from "components/common/Icon";
import { TextColor } from "components/models/common";
import React from "react";
import IconName from "typings/server/icons";

interface DatabaseIconProps {
    databaseName: string;
    isSharded?: boolean;
}

export default function DatabaseIcon({ databaseName, isSharded }: DatabaseIconProps) {
    if (!databaseName) {
        return <Icon icon="database" addon="cancel" />;
    }

    const addon: IconName = isSharded ? "sharding" : null;
    const color: TextColor = isSharded ? "shard" : "primary";

    return <Icon icon="database" addon={addon} color={color} />;
}
