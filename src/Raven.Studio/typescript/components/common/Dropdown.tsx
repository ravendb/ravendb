import classNames from "classnames";
import Button from "react-bootstrap/Button";
import { ButtonProps } from "react-bootstrap/Button";
import Dropdown, { DropdownProps } from "react-bootstrap/Dropdown";
import { DropdownMenuProps } from "react-bootstrap/DropdownMenu";
import { createContext, forwardRef, useContext, useState } from "react";
import { createPortal } from "react-dom";
import "./Dropdown.scss";

interface CustomDropdownToggleProps extends ButtonProps {
    isCaretHidden?: boolean;
}

interface DropdownPortalMenuContextValue {
    show: boolean;
}

type DropdownPortalMenuProps = Omit<DropdownMenuProps, "as">;

const DropdownPortalMenuContext = createContext<DropdownPortalMenuContextValue>(null);

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

export function DropdownWithPortalMenu({ children, defaultShow, onToggle, show: showProp, ...props }: DropdownProps) {
    const [uncontrolledShow, setUncontrolledShow] = useState(defaultShow ?? false);
    const show = showProp ?? uncontrolledShow;

    const handleToggle: DropdownProps["onToggle"] = (nextShow, meta) => {
        if (showProp == null) {
            setUncontrolledShow(nextShow);
        }

        onToggle?.(nextShow, meta);
    };

    return (
        <DropdownPortalMenuContext.Provider value={{ show }}>
            <Dropdown show={show} onToggle={handleToggle} {...props}>
                {children}
            </Dropdown>
        </DropdownPortalMenuContext.Provider>
    );
}

export function DropdownPortalMenu(props: DropdownPortalMenuProps) {
    const context = useContext(DropdownPortalMenuContext);
    const show = props.show ?? context?.show;

    if (!show) {
        return null;
    }

    return createPortal(
        <div className="bs5">
            <Dropdown.Menu {...props} />
        </div>,
        document.body
    );
}
