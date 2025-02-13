import { Meta, StoryObj } from "@storybook/react";
import React, { ComponentProps, useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import DatePicker from "./DatePicker";
import moment from "moment";

export default {
    title: "Bits/DatePicker",
    component: DatePicker,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-4805",
        },
    },
} satisfies Meta<typeof DatePicker>;

function DatePickerWithShownValue(args: ComponentProps<typeof DatePicker>) {
    const [startDate, setStartDate] = useState(new Date());

    return (
        <div>
            <DatePicker selected={startDate} onChange={(date: Date) => setStartDate(date)} {...args} />
            <hr />
            <div>Selected value: {startDate.toString()}</div>
        </div>
    );
}

export const Primary: StoryObj<ComponentProps<typeof DatePicker>> = {
    name: "Date Picker",
    render: DatePickerWithShownValue,
    args: {
        dateFormat: "dd/MM/yyyy HH:mm",
        timeFormat: "HH:mm",
        maxDate: moment().add(2, "days").toDate(),
        minDate: moment().add(-2, "months").toDate(),
        showTimeSelect: false,
        showTimeInput: false,
        shouldCloseOnSelect: false,
        showDisabledMonthNavigation: false,
        showFullMonthYearPicker: false,
        showMonthDropdown: false,
        showMonthYearDropdown: false,
        showMonthYearPicker: false,
        showPopperArrow: false,
        showPreviousMonths: false,
        showQuarterYearPicker: false,
        showTimeSelectOnly: false,
        showTwoColumnMonthYearPicker: false,
        showFourColumnMonthYearPicker: false,
        showWeekNumbers: false,
        showYearDropdown: false,
        showYearPicker: false,
        showIcon: false,
        invalid: false,
    },
};
