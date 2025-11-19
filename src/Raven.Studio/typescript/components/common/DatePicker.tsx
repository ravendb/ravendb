import React, { ComponentProps, forwardRef } from "react";
import ReactDatePicker from "react-datepicker";
import Form from "react-bootstrap/Form";
import "./ReactDatepicker.scss";

const DatePickerInput = forwardRef<HTMLInputElement, ComponentProps<typeof Form.Control>>(
    function DatePickerInput(props, ref) {
        return <Form.Control ref={ref} {...props} />;
    }
);

export default function DatePicker(props: ComponentProps<typeof ReactDatePicker> & { isInvalid?: boolean }) {
    const dateFormat = props.dateFormat ?? (props.showTimeSelect ? "dd/MM/yyyy HH:mm" : "dd/MM/yyyy");
    const timeFormat = props.timeFormat ?? "HH:mm";

    return (
        <ReactDatePicker
            {...props}
            customInput={<DatePickerInput isInvalid={props.isInvalid} />}
            dateFormat={dateFormat}
            timeFormat={timeFormat}
        />
    );
}
