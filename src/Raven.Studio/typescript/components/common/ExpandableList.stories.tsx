import { Meta, StoryObj } from "@storybook/react-webpack5";
import { useState } from "react";
import Badge from "react-bootstrap/Badge";
import ExpandableList from "components/common/ExpandableList";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/ExpandableList",
    decorators: [withStorybookContexts, withBootstrap5],
    component: ExpandableList,
} satisfies Meta<typeof ExpandableList>;

const sampleItems = [
    "Users/1-A",
    "Orders/830-A",
    "Companies/91-A",
    "Products/14-A",
    "Employees/7-A",
    "Suppliers/22-A",
    "Categories/4-A",
    "Regions/2-A",
];

interface ExpandableListStoryProps {
    itemsCount: number;
    collapsedItemsCount: number;
    initiallyExpanded?: boolean;
}

function ExpandableListStory({ itemsCount, collapsedItemsCount, initiallyExpanded = false }: ExpandableListStoryProps) {
    const [isExpanded, setIsExpanded] = useState(initiallyExpanded);
    const items = sampleItems.slice(0, itemsCount);

    return (
        <div className="w-50">
            <ExpandableList
                className="panel-bg-2 rounded p-2"
                contentClassName="vstack gap-1"
                itemsCount={items.length}
                collapsedItemsCount={collapsedItemsCount}
                isExpanded={isExpanded}
                setIsExpanded={setIsExpanded}
            >
                {({ visibleCount, hiddenCount, isExpanded }) => (
                    <>
                        {items.slice(0, visibleCount).map((item) => (
                            <div key={item} className="hstack justify-content-between bg-body rounded px-2 py-1">
                                <span>{item}</span>
                                <Badge bg="faded-primary">Document</Badge>
                            </div>
                        ))}
                        <div className="text-muted small">
                            Visible: {visibleCount}, hidden: {hiddenCount}, expanded: {isExpanded ? "yes" : "no"}
                        </div>
                    </>
                )}
            </ExpandableList>
        </div>
    );
}

export const Default: StoryObj<ExpandableListStoryProps> = {
    args: {
        itemsCount: 8,
        collapsedItemsCount: 4,
    },
    argTypes: {
        itemsCount: { control: { type: "range", min: 1, max: sampleItems.length, step: 1 } },
        collapsedItemsCount: { control: { type: "range", min: 1, max: sampleItems.length, step: 1 } },
    },
    render: (args) => <ExpandableListStory {...args} />,
};

export const Expanded: StoryObj<ExpandableListStoryProps> = {
    args: {
        itemsCount: 8,
        collapsedItemsCount: 4,
        initiallyExpanded: true,
    },
    render: (args) => <ExpandableListStory {...args} />,
};

export const WithoutToggle: StoryObj<ExpandableListStoryProps> = {
    args: {
        itemsCount: 4,
        collapsedItemsCount: 6,
    },
    render: (args) => <ExpandableListStory {...args} />,
};
