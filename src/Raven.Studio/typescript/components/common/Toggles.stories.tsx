import { InputItem } from "components/models/common";
import React, { useState } from "react";
import { Card } from "reactstrap";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { MultiCheckboxToggle } from "./MultiCheckboxToggle";
import { MultiRadioToggle } from "./MultiRadioToggle";

export default {
    title: "Bits/Toggles",
    decorators: [withStorybookContexts, withBootstrap5],
};

export function Toggles() {
    const [radioSelectedItem, setRadioSelectedItem] = useState<string>(null);
    const [checkboxWithoutAllSelectedItems, setCheckboxWithoutAllSelectedItems] = useState<string[]>([]);
    const [checkboxWithAllSelectedItems, setCheckboxWithAllSelectedItems] = useState<string[]>([]);

    const radioList: InputItem[] = [
        { value: "1hour", label: "1 Hour" },
        { value: "6hours", label: "6 hours" },
        { value: "12hours", label: "12 hours" },
        { value: "1day", label: "1 day" },
    ];

    const checkboxList: InputItem[] = [
        { value: "normal", label: "Normal" },
        { value: "error", label: "Error/Faulty", count: 3 },
        { value: "stale", label: "Stale" },
        { value: "rolling", label: "Rolling deployment", count: 1 },
        { value: "idle", label: "Idle", count: 0 },
        { value: "local", label: "Local", verticalSeparatorLine: true },
        { value: "remote", label: "Remote" },
    ];

    return (
        <Card>
            <MultiRadioToggle
                className="p-4"
                inputItems={radioList}
                label="Multi Radio Toggle"
                selectedItem={radioSelectedItem}
                setSelectedItem={(x) => setRadioSelectedItem(x)}
            />
            <MultiCheckboxToggle
                className="p-4"
                inputItems={checkboxList}
                label="Multi Checkbox Toggle without select all"
                selectedItems={checkboxWithoutAllSelectedItems}
                setSelectedItems={(x) => setCheckboxWithoutAllSelectedItems(x)}
            />
            <MultiCheckboxToggle
                className="p-4"
                inputItems={checkboxList}
                label="Multi Checkbox Toggle with select all"
                selectedItems={checkboxWithAllSelectedItems}
                setSelectedItems={(x) => setCheckboxWithAllSelectedItems(x)}
                selectAll
            />
            <MultiCheckboxToggle
                className="p-4"
                inputItems={checkboxList}
                label="Multi Checkbox Toggle with select all + custom label"
                selectedItems={checkboxWithAllSelectedItems}
                setSelectedItems={(x) => setCheckboxWithAllSelectedItems(x)}
                selectAll
                selectAllLabel="Select All"
            />

            <MultiCheckboxToggle
                className="p-4"
                inputItems={checkboxList}
                label="Multi Checkbox Toggle with select all + custom label + count"
                selectedItems={checkboxWithAllSelectedItems}
                setSelectedItems={(x) => setCheckboxWithAllSelectedItems(x)}
                selectAll
                selectAllLabel="Select All Items"
                selectAllCount={3}
            />
        </Card>
    );
}
