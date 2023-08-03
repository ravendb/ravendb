import { Meta, StoryObj } from "@storybook/react";
import React, { useEffect, useState } from "react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import VirtualGrid, { useVirtualGrid } from "./VirtualGrid";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import { Button } from "reactstrap";

interface SomeData {
    id: number;
    name: string;
}

export default {
    title: "Bits",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof VirtualGrid>;

function DefaultView() {
    const [counter, setCounter] = useState<number>(0);

    const fetcher = () => {
        return $.Deferred<pagedResult<SomeData>>().resolve({
            items: [
                { id: 1, name: `First count ${counter}` },
                { id: 2, name: `Second count ${counter * 2}` },
            ],
            totalResultCount: 2,
        });
    };

    const columnsProvider = (): virtualColumn[] => {
        return [
            new textColumn<SomeData>(gridController, (x) => x.id, "ID", "15%", {
                sortable: "number",
            }),
            new textColumn<SomeData>(gridController, (x) => x.name, "Name", "85%", {
                sortable: "string",
            }),
        ];
    };

    const { gridController, setGridController } = useVirtualGrid({ fetcher, columnsProvider });

    useEffect(() => {
        gridController?.reset();
    }, [counter, gridController]);

    return (
        <div>
            <Button onClick={() => setCounter((x) => x + 1)}>Counter {counter}</Button>
            <VirtualGrid<SomeData> setGridController={setGridController} />
        </div>
    );
}

export const Default: StoryObj<typeof VirtualGrid> = {
    name: "Virtual Grid",
    render: DefaultView,
};
