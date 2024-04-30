import virtualGridController from "widgets/virtualGrid/virtualGridController";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import React, { useEffect, useState } from "react";
import VirtualGrid from "components/common/VirtualGrid";
import { Icon } from "components/common/Icon";

interface ServerSettingsVirtualGridProps {
    configurationKey: string;
    effectiveValue: string;
    origin: string;
}

export function ServerSettingsVirtualGrid(props: ServerSettingsVirtualGridProps) {
    const [counter, setCounter] = useState<number>(0);
    const [gridController, setGridController] = useState<virtualGridController<ServerSettingsVirtualGridProps>>();
    useEffect(() => {
        if (!gridController) {
            return;
        }

        gridController.headerVisible(true);
        gridController.init(
            () => fetcher(counter),
            () => columnsProvider(gridController)
        );
    }, [counter, gridController]);

    useEffect(() => {
        gridController?.reset();
    }, [counter, gridController]);
    return <VirtualGrid<ServerSettingsVirtualGridProps> setGridController={setGridController} />;
}

const fetcher = (counter: number) => {
    return $.Deferred<pagedResult<ServerSettingsVirtualGridProps>>().resolve({
        items: new Array(10).fill(null).map((_, id) => ({
            configurationKey: `License.Eula.Accepted`,
            effectiveValue: `True`,
            origin: `Default`,
        })),
        totalResultCount: 30,
    });
};

const columnsProvider = (gridController: virtualGridController<ServerSettingsVirtualGridProps>): virtualColumn[] => {
    return [
        new textColumn<ServerSettingsVirtualGridProps>(
            gridController,
            (x) => x.configurationKey,
            "Configuration key",
            "34%",
            {
                sortable: "string",
            }
        ),
        new textColumn<ServerSettingsVirtualGridProps>(
            gridController,
            (x) => x.effectiveValue,
            "Effective value",
            "33%",
            {
                sortable: "string",
            }
        ),
        new textColumn<ServerSettingsVirtualGridProps>(gridController, (x) => x.origin, "Origin", "33%", {
            sortable: "string",
        }),
    ];
};

ServerSettingsVirtualGrid.defaultProps = {
    configurationKey: "defaultKey", // Example default value
    effectiveValue: "defaultValue", // Example default value
    origin: "defaultOrigin", // Example default value
};
