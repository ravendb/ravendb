import type { Meta, StoryObj } from "@storybook/react-vite";
import { ApiState } from "./api-state";
import { CardListSkeleton, ChartSkeleton, DetailGridSkeleton, FormFieldsSkeleton } from "./loading-skeletons";
import { SectionTableSkeleton } from "@/components/table/section-table";

const AGENT_TABLE_HEADERS = ["Agent name", "Status", "Model", "Last run", "Conversations", "Tokens", ""];

const meta = {
    title: "Components/Api State",
    component: ApiState,
    decorators: [
        (Story) => (
            <div className="max-w-3xl p-6">
                <Story />
            </div>
        ),
    ],
    argTypes: {
        isLoading: { control: "boolean", description: "Toggle to see the loading state." },
        isError: { control: "boolean" },
        loadingLabel: { control: "text" },
        errorTitle: { control: "text" },
        // Neither a node nor a callback is editable as a control; Playground swaps the skeleton
        // through a mapped select instead.
        skeleton: { control: false },
        children: { control: false },
        onRetry: { control: false },
    },
    args: {
        isLoading: true,
        children: <p className="text-sm">The loaded content.</p>,
    },
} satisfies Meta<typeof ApiState>;

export default meta;

type Story = StoryObj<typeof meta>;

/**
 * Every state in one place: toggle `isLoading` / `isError` and pick a skeleton shape from the
 * controls panel. The other stories pin one shape each so the test run covers all of them.
 */
export const Playground: Story = {
    argTypes: {
        skeleton: {
            options: ["table", "cardList", "form", "detailGrid", "chart", "none"],
            mapping: {
                table: <SectionTableSkeleton headers={AGENT_TABLE_HEADERS} />,
                cardList: <CardListSkeleton />,
                form: <FormFieldsSkeleton />,
                detailGrid: <DetailGridSkeleton count={2} />,
                chart: <ChartSkeleton />,
                none: undefined,
            },
            control: {
                type: "select",
                labels: {
                    table: "Table",
                    cardList: "Card list",
                    form: "Form fields",
                    detailGrid: "Detail grid",
                    chart: "Chart",
                    none: "None — falls back to the text label",
                },
            },
        },
    },
    args: {
        // Both flags are set so the panel offers real toggles rather than "Set boolean" buttons.
        isLoading: true,
        isError: false,
        skeleton: "table",
        loadingLabel: "Loading agents...",
        errorTitle: "Could not load agents",
        onRetry: () => {},
    },
};

export const TableLoading: Story = {
    args: {
        loadingLabel: "Loading agents...",
        skeleton: <SectionTableSkeleton headers={AGENT_TABLE_HEADERS} />,
    },
};

export const CardListLoading: Story = {
    args: { loadingLabel: "Loading errors...", skeleton: <CardListSkeleton /> },
};

export const FormLoading: Story = {
    args: { loadingLabel: "Loading agent...", skeleton: <FormFieldsSkeleton /> },
};

export const DetailGridLoading: Story = {
    args: { loadingLabel: "Resolving DNS…", skeleton: <DetailGridSkeleton count={2} /> },
};

export const ChartLoading: Story = {
    args: { loadingLabel: "Loading chart…", skeleton: <ChartSkeleton /> },
};

// No skeleton: the shape is not knowable ahead of the data, so the wait is stated in words.
export const UnknownShapeLoading: Story = {
    args: { loadingLabel: "Connecting to live CDC performance..." },
};

export const ErrorState: Story = {
    args: { isLoading: false, isError: true, errorTitle: "Could not load agents", onRetry: () => {} },
};

export const Loaded: Story = {
    args: { isLoading: false },
};
