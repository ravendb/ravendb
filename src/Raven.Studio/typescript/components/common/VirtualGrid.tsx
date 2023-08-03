import React, { createRef, useEffect, useState } from "react";
import { ReactToKnockoutComponent } from "common/reactUtils";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";

interface VirtualGridProps<T> {
    setGridController: (gridController: virtualGridController<T>) => void;
    height?: string;
}

export default class VirtualGrid<T> extends ReactToKnockoutComponent<VirtualGridProps<T>> {
    ref = createRef<HTMLDivElement>();
    height = this.props.height ?? "300px";

    render() {
        return (
            <div
                ref={this.ref}
                dangerouslySetInnerHTML={{
                    __html: `<virtual-grid style="height: ${this.height}"  class="resizable flex-window" params="controller: props.setGridController"></virtual-grid>`,
                }}
            ></div>
        );
    }
}

interface UseVirtualGridProps<T> {
    fetcher: (skip: number, pageSize: number) => JQueryPromise<pagedResult<T>>;
    columnsProvider: (containerWidth: number, results: pagedResult<T>) => virtualColumn[];
}

export function useVirtualGrid<T>({ fetcher, columnsProvider }: UseVirtualGridProps<T>) {
    const [gridController, setGridController] = useState<virtualGridController<T>>();

    useEffect(() => {
        if (!gridController) {
            return;
        }

        gridController.headerVisible(true);
        gridController.init(fetcher, columnsProvider);
    }, [columnsProvider, fetcher, gridController]);

    return { gridController, setGridController };
}
