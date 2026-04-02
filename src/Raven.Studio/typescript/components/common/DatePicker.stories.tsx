import { Meta, StoryObj } from "@storybook/react-webpack5";
import { useState } from "react";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import DatePicker, { DatePickerProps } from "./DatePicker";
import moment from "moment";

export default {
    title: "Bits/DatePicker",
    component: DatePicker,
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-4805",
        },
    },
} satisfies Meta<typeof DatePicker>;

type SingleDatePickerProps = Extract<
    DatePickerProps,
    {
        selectsRange?: false | undefined;
        selectsMultiple?: false | undefined;
    }
>;

function DatePickerWithShownValue(args: SingleDatePickerProps) {
    const [startDate, setStartDate] = useState<Date>(args.selected ?? new Date());

    const handleChange: SingleDatePickerProps["onChange"] = (date) => {
        setStartDate(date);
    };

    return (
        <div>
            <DatePicker {...args} selected={startDate} onChange={handleChange} />
            <hr />
            <div>Selected value: {startDate ? startDate.toString() : "null"}</div>
        </div>
    );
}

export const Primary: StoryObj<DatePickerProps> = {
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
        isInvalid: false,
    },
};
