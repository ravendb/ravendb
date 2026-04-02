import { ComponentProps, forwardRef } from "react";
import ReactDatePicker, { type DatePickerProps as ReactDatePickerProps } from "react-datepicker";
import Form from "react-bootstrap/Form";
import "./ReactDatepicker.scss";

const DatePickerInput = forwardRef<HTMLInputElement, ComponentProps<typeof Form.Control>>(
    function DatePickerInput(props, ref) {
        return <Form.Control ref={ref} {...props} />;
    }
);

export type DatePickerProps = ReactDatePickerProps & {
    isInvalid?: boolean;
};

export default function DatePicker(props: DatePickerProps) {
    const {
        isInvalid,
        dateFormat = props.showTimeSelect ? "dd/MM/yyyy HH:mm" : "dd/MM/yyyy",
        timeFormat = "HH:mm",
        popperPlacement = "bottom-start",
        portalId = "react-datepicker-portal",
        popperProps,
        ...rest
    } = props;

    const reactDatePickerProps: ReactDatePickerProps = {
        customInput: <DatePickerInput isInvalid={isInvalid} />,
        dateFormat,
        timeFormat,
        popperPlacement,
        portalId,
        popperProps: {
            strategy: "fixed",
            ...popperProps,
        },
        ...rest,
    };

    return <ReactDatePicker {...reactDatePickerProps} />;
}
