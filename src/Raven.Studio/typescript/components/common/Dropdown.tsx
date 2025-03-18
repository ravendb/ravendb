import classNames from "classnames";
import Button from "react-bootstrap/Button";
import { forwardRef } from "react";
import "./Dropdown.scss";

interface CustomDropdownToggleProps extends Button.BtnProps {
    isCaretHidden?: boolean;
}

export const CustomDropdownToggle = forwardRef<HTMLButtonElement, CustomDropdownToggleProps>(
    ({ children, className, isCaretHidden, ...props }, ref) => {
        return (
            <Button
                variant="secondary"
                ref={ref}
                className={classNames("custom-dropdown-toggle", className, { "no-caret": isCaretHidden })}
                {...props}
            >
                {children}
            </Button>
        );
    }
);

CustomDropdownToggle.displayName = "CustomDropdownToggle";
