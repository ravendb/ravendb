import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import React, { useMemo } from "react";
import { Icon } from "components/common/Icon";
import IconName from "../../../../../../typings/server/icons";
import genUtils from "common/generalUtils";
import { configurationOrigin } from "models/database/settings/databaseSettingsModels";
import { CellWithDescription, CellWithDescriptionProps } from "components/common/virtualTable/cells/CellWithCopy";

export interface ServerSettingsColumns {
    configurationKey: string;
    effectiveValue: string | null;
    configurationKeyTooltip?: string;
    origin: configurationOrigin;
}

export function useServerSettingsColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const serverSettingsColumns: ColumnDef<ServerSettingsColumns>[] = useMemo(
        () => [
            {
                header: "Configuration Key",
                accessorKey: "configurationKey",
                cell: CellConfigurationKeyValueWrapper,
                size: getSize(33.33),
            },
            {
                header: "Effective Value",
                accessorKey: "effectiveValue",
                cell: CellEffectiveValueWrapper,
                size: getSize(33.33),
            },
            {
                header: "Origin",
                accessorKey: "origin",
                cell: CellOriginValueWrapper,
                size: getSize(33.33),
            },
        ],
        [getSize]
    );

    return serverSettingsColumns;
}

function CellValueWrapper(props: CellWithDescriptionProps<ServerSettingsColumns, ServerSettingsColumns[keyof ServerSettingsColumns]>) {
  const { origin } = props.cell.row.original;
  const cellClass = origin === "Server" ? "text-warning" : "";

  return <CellWithDescription<ServerSettingsColumns, ServerSettingsColumns[keyof ServerSettingsColumns]>
    cellClassName={cellClass} {...props} />;
}


function CellConfigurationKeyValueWrapper({
    getValue,
    ...props
}: CellContext<ServerSettingsColumns, ServerSettingsColumns["configurationKey"]>) {
  return <CellValueWrapper getValue={getValue}
                           description={genUtils.unescapeHtml(props.cell.row.original.configurationKeyTooltip)} {...props} />;
}

function CellEffectiveValueWrapper({
    getValue,
    ...props
}: CellContext<ServerSettingsColumns, ServerSettingsColumns["effectiveValue"]>) {
  return <CellValueWrapper getValue={getValue}
                           {...props} />;
}

const titleValueField: Record<ServerSettingsColumns["origin"], string> = {
    Server: "Value has been customized, overriding the default settings",
    Default: "No customized value has been set",
    Database: "",
};

function CellOriginValueWrapper({
    getValue,
    ...props
}: CellContext<ServerSettingsColumns, ServerSettingsColumns["origin"]>) {
  const conditionalIconName: IconName = getValue() === "Server" ? "server" : "default";
  return (
    <>
      <Icon icon={conditionalIconName} />
      <CellValueWrapper getValue={getValue}
                        showActionsMenu={false}
                        description={titleValueField[getValue()]}
                        {...props} />
    </>
  );
}
