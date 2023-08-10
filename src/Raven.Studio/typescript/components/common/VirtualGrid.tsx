import React, { createRef } from "react";
import { ReactToKnockoutComponent } from "common/reactUtils";
import virtualGridController from "widgets/virtualGrid/virtualGridController";

interface VirtualGridProps<T> {
    setGridController: (gridController: virtualGridController<T>) => void;
}

export default class VirtualGrid<T> extends ReactToKnockoutComponent<VirtualGridProps<T>> {
    ref = createRef<HTMLDivElement>();

    render() {
        return (
            <div
                ref={this.ref}
                dangerouslySetInnerHTML={{
                    __html: `
                        <virtual-grid
                            class="resizable"
                            params="controller: props.setGridController">
                        </virtual-grid>
                    `,
                }}
            ></div>
        );
    }
}
