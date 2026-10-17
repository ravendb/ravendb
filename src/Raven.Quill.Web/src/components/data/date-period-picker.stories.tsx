import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Heading } from "@/components/typography";
import { getDefaultDatePeriod, type DatePeriod } from "@/lib/date-period";
import { DatePeriodPicker } from "./date-period-picker";

const EARLIEST = new Date(Date.UTC(2024, 2, 14));

function PickerPage() {
    const [period, setPeriod] = useState<DatePeriod>(getDefaultDatePeriod);

    return (
        <div className="flex items-center justify-between gap-3 p-6">
            <Heading as="h1" variant="page">
                Usage
            </Heading>
            <DatePeriodPicker value={period} earliest={EARLIEST} onChange={setPeriod} />
        </div>
    );
}

const meta = {
    title: "Components/DatePeriodPicker",
    component: PickerPage,
    parameters: { layout: "fullscreen" },
} satisfies Meta<typeof PickerPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
