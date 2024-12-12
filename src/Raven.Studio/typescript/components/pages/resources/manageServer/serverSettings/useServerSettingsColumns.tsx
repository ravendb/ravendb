import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import { ComponentPropsWithoutRef, useMemo } from "react";
import { Icon } from "components/common/Icon";
import IconName from "../../../../../../typings/server/icons";
import genUtils from "common/generalUtils";
import { configurationOrigin } from "models/database/settings/databaseSettingsModels";

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

type CellValueWrapperProps = Partial<
    CellContext<ServerSettingsColumns, ServerSettingsColumns[keyof ServerSettingsColumns]>
> &
    ComponentPropsWithoutRef<typeof CellValue>;

function CellValueWrapper({ cell, value, ...props }: CellValueWrapperProps) {
    const { origin } = cell.row.original;
    const cellClass = origin === "Server" ? "text-warning" : "";

    return <CellValue className={cellClass} value={value} {...props} />;
}

function CellConfigurationKeyValueWrapper({
    getValue,
    ...props
}: CellContext<ServerSettingsColumns, ServerSettingsColumns["configurationKey"]>) {
    return <CellValueWrapper value={getValue()} title={props.cell.row.original.configurationKeyTooltip} {...props} />;
}

function CellEffectiveValueWrapper({
    getValue,
    ...props
}: CellContext<ServerSettingsColumns, ServerSettingsColumns["effectiveValue"]>) {
    return (
        <CellValueWrapper
            value={genUtils.unescapeHtml(getValue())}
            title={genUtils.unescapeHtml(getValue())}
            {...props}
        />
    );
}

const titleValueField: Record<ServerSettingsColumns["origin"], string> = {
    Server: "Value is configured in the settings.json file, overriding the default settings",
    Default: "No customized value is set",
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
            <CellValueWrapper value={getValue()} title={titleValueField[getValue()]} {...props} />
        </>
    );
}
