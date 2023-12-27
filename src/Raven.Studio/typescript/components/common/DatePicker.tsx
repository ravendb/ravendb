import React, { ComponentProps, forwardRef } from "react";
import ReactDatePicker from "react-datepicker";
import { Input } from "reactstrap";
import "./ReactDatepicker.scss";

const DatePickerInput = forwardRef<HTMLInputElement, ComponentProps<typeof Input>>(function DatePickerInput(
    props,
    ref
) {
    return <Input innerRef={ref} {...props} />;
});

export default function DatePicker(props: ComponentProps<typeof ReactDatePicker> & { invalid?: boolean }) {
    const dateFormat = props.dateFormat ?? (props.showTimeSelect ? "dd/MM/yyyy HH:mm" : "dd/MM/yyyy");
    const timeFormat = props.timeFormat ?? "HH:mm";

    return (
        <ReactDatePicker
            {...props}
            customInput={<DatePickerInput invalid={props.invalid} />}
            dateFormat={dateFormat}
            timeFormat={timeFormat}
        />
    );
}
