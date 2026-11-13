import { Meta, StoryObj } from "@storybook/react-webpack5";
import { PaginationBar as PaginationBarComponent, PageNavigation as PageNavigationComponent } from "./PaginationBar";
import { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Pagination",
    decorators: [withStorybookContexts, withBootstrap5],
    component: PaginationBarComponent,
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=10-101",
        },
    },
} satisfies Meta<typeof PaginationBarComponent>;

const totalCount = 1_059;

export function PaginationBar() {
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(10);

    const pageCount = Math.ceil(totalCount / pageSize);
    const firstItemNumber = (page - 1) * pageSize + 1;

    return (
        <PaginationBarComponent
            pagination={{
                page,
                totalPages: pageCount,
                firstItemNumber,
                lastItemNumber: Math.min(page * pageSize, totalCount),
                totalCount,
                onPageChange: setPage,
                pageSizeSelector: {
                    pageSize,
                    options: [10, 25, 50, 100],
                    onChange: (nextPageSize) => {
                        setPageSize(nextPageSize);
                        setPage(1);
                    },
                },
            }}
        />
    );
}

interface PageNavigationStoryArgs {
    totalPages: number;
    isSimple: boolean;
}

function PageNavigationExample({ totalPages, isSimple }: PageNavigationStoryArgs) {
    const [page, setPage] = useState(1);

    return (
        <PageNavigationComponent
            page={Math.min(page, totalPages)}
            totalPages={totalPages}
            onPageChange={setPage}
            isSimple={isSimple}
        />
    );
}

export const PageNavigation: StoryObj<PageNavigationStoryArgs> = {
    render: (args) => <PageNavigationExample {...args} />,
    args: {
        totalPages: 12,
        isSimple: false,
    },
};

export const SimplePageNavigation: StoryObj<PageNavigationStoryArgs> = {
    render: (args) => <PageNavigationExample {...args} />,
    args: {
        totalPages: 12,
        isSimple: true,
    },
};
