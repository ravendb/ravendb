import { Meta, StoryObj } from "@storybook/react-webpack5";
import { PageNavigation as PageNavigationComponent } from "./PaginationBar";
import { useState } from "react";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";

interface PageNavigationStoryArgs {
    totalPages: number;
    isSimple: boolean;
}

function PageNavigationExample({ totalPages, isSimple }: PageNavigationStoryArgs) {
    const [page, setPage] = useState(1);

    return <PageNavigationComponent page={page} totalPages={totalPages} onPageChange={setPage} isSimple={isSimple} />;
}

export default {
    title: "Bits/Pagination/Page Navigation",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args) => <PageNavigationExample {...args} />,
    args: {
        totalPages: 12,
        isSimple: false,
    },
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=10-101",
        },
    },
} satisfies Meta<PageNavigationStoryArgs>;

type Story = StoryObj<PageNavigationStoryArgs>;

export const Default: Story = {};

export const Simple: Story = {
    args: { isSimple: true },
};

export const SimpleFewPages: Story = {
    args: { isSimple: true, totalPages: 4 },
};

export const SinglePage: Story = {
    args: { totalPages: 1 },
};

export const LongValues: Story = {
    args: { totalPages: 987_654_321 },
};
