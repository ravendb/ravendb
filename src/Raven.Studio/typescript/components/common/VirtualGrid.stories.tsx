import { Meta, StoryObj } from "@storybook/react";
import React, { useEffect, useState } from "react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import VirtualGrid from "./VirtualGrid";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import { Button } from "reactstrap";
import virtualGridController from "widgets/virtualGrid/virtualGridController";

interface SomeData {
    id: number;
    name: string;
}

export default {
    title: "Bits",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof VirtualGrid>;

export const Default: StoryObj<typeof VirtualGrid> = {
    name: "Virtual Grid",
    render: DefaultView,
};

function DefaultView() {
    const [counter, setCounter] = useState<number>(0);

    const [gridController, setGridController] = useState<virtualGridController<SomeData>>();

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

    return (
        <div className="p-4 flex-window stretch">
            <div className="flex-window-head">
                <Button onClick={() => setCounter((x) => x + 1)}>Counter {counter}</Button>
            </div>

            <div className="flex-window-scroll mt-2">
                <div className="scroll-stretch">
                    <div className="panel">
                        <VirtualGrid<SomeData> setGridController={setGridController} />
                    </div>
                </div>
            </div>
        </div>
    );
}

const fetcher = (counter: number) => {
    return $.Deferred<pagedResult<SomeData>>().resolve({
        items: new Array(30).fill(null).map((_, id) => ({
            id,
            name: `counter name: ${counter * id}`,
        })),
        totalResultCount: 30,
    });
};

const columnsProvider = (gridController: virtualGridController<SomeData>): virtualColumn[] => {
    return [
        new textColumn<SomeData>(gridController, (x) => x.id, "ID", "15%", {
            sortable: "number",
        }),
        new textColumn<SomeData>(gridController, (x) => x.name, "Name", "85%", {
            sortable: "string",
        }),
    ];
};
