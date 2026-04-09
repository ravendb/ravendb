import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import AceEditor from "./AceEditor";
import AceUnifiedDiff from "./AceUnifiedDiff";
import ReactAce from "react-ace/lib/ace";
import { useRef, useState } from "react";

export default {
    title: "Bits/AceEditor",
    component: AceEditor,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=3-16049",
        },
    },
} satisfies Meta;

export const Default: StoryObj = {
    name: "Ace Editor",
    render: () => <JsonEditorComponent />,
};

function JsonEditorComponent() {
    const aceRef = useRef<ReactAce>(null);
    const [aceValue, setAceValue] = useState("");

    return (
        <AceEditor
            aceRef={aceRef}
            mode="json"
            actions={[
                { component: <AceEditor.FullScreenAction /> },
                { component: <AceEditor.FormatAction /> },
                { component: <AceEditor.ToggleNewLinesAction /> },
                { component: <AceEditor.AutoResizeHeightAction /> },
            ]}
            value={aceValue}
            onChange={(value) => setAceValue(value)}
            height="300px"
        />
    );
}

export const UnifiedDiff: StoryObj = {
    render: () => {
        const oldDoc = `{
    "Name": "Original Frankfurter grüne Soße",
    "Supplier": "suppliers/12-A",
    "Category": "categories/2-A",
    "QuantityPerUnit": "12 boxes",
    "PricePerUnit": 13,
    "UnitsInStock": 12,
    "UnitsOnOrder": 32,
    "Discontinued": false,
    "ReorderLevel": 15,
}`;

        const newDoc = `{
    "Name": "Original Frankfurter grüne Soße",
    "Supplier": "suppliers/12-A",
    "Category": "categories/2-A",
    "QuantityPerUnit": "12 boxes",
    "PricePerUnit": 16,
    "UnitsInStock": 12,
    "Discontinued": false,
    "ReorderLevel": 15,
    "@metadata": {
        "@collection": "Products",
    }
}`;

        return <AceUnifiedDiff mode="json" value1={oldDoc} value2={newDoc} height="400px" />;
    },
};
