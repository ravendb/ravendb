import React, { createRef } from "react";
import { ReactToKnockoutComponent } from "common/reactUtils";
import virtualGridController from "widgets/virtualGrid/virtualGridController";

interface VirtualGridProps<T> {
    setGridController: (gridController: virtualGridController<T>) => void;
    height?: string;
}

export default class VirtualGrid<T> extends ReactToKnockoutComponent<VirtualGridProps<T>> {
    ref = createRef<HTMLDivElement>();
    height = this.props.height ?? "300px";

    // TODO kalczur fix height
    render() {
        return (
            <div
                ref={this.ref}
                dangerouslySetInnerHTML={{
                    __html: `<virtual-grid class="resizable flex-window" params="controller: props.setGridController"></virtual-grid>`,
                }}
            ></div>
        );
    }
}
