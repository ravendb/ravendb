import { Meta, StoryObj } from "@storybook/react-webpack5";
import { PaginationBar as PaginationBarComponent } from "./PaginationBar";
import { useState } from "react";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

interface PaginationBarStoryArgs {
    totalCount: number;
    initialPage: number;
    isTotalUnknown: boolean;
    hasPageSizeSelector: boolean;
}

function PaginationBarExample({
    totalCount,
    initialPage,
    isTotalUnknown,
    hasPageSizeSelector,
}: PaginationBarStoryArgs) {
    const [page, setPage] = useState(initialPage);
    const [pageSize, setPageSize] = useState(PAGE_SIZE_OPTIONS[0]);

    const knownTotalPages = Math.ceil(totalCount / pageSize);
    const hasMorePages = page < knownTotalPages;
    const firstItemNumber = totalCount === 0 ? 0 : (page - 1) * pageSize + 1;

    return (
        <PaginationBarComponent
            pagination={{
                page,
                totalPages: isTotalUnknown ? page + (hasMorePages ? 1 : 0) : knownTotalPages,
                firstItemNumber,
                lastItemNumber: Math.min(page * pageSize, totalCount),
                totalCount: isTotalUnknown ? null : totalCount,
                onPageChange: setPage,
                pageSizeSelector: hasPageSizeSelector
                    ? {
                          pageSize,
                          options: PAGE_SIZE_OPTIONS,
                          onChange: (nextPageSize) => {
                              setPageSize(nextPageSize);
                              setPage(1);
                          },
                      }
                    : undefined,
            }}
        />
    );
}

export default {
    title: "Bits/Pagination/Pagination Bar",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args) => <PaginationBarExample {...args} />,
    args: {
        totalCount: 1_059,
        initialPage: 1,
        isTotalUnknown: false,
        hasPageSizeSelector: true,
    },
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=10-101",
        },
    },
} satisfies Meta<PaginationBarStoryArgs>;

type Story = StoryObj<PaginationBarStoryArgs>;

export const Default: Story = {};

export const Empty: Story = {
    args: { totalCount: 0 },
};

export const SinglePage: Story = {
    args: { totalCount: 8 },
};

export const UnknownTotal: Story = {
    args: { isTotalUnknown: true },
};

export const WithoutPageSizeSelector: Story = {
    args: { hasPageSizeSelector: false },
};

export const LongValues: Story = {
    args: { totalCount: 9_876_543_210, initialPage: 987_654_321 },
};
