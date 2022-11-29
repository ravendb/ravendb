import { useCallback, useEffect, useState } from "react";
import classNames from "classnames";
import React from "react";
import { usePopper } from "react-popper";
import { Button } from "reactstrap";
import useBoolean from "hooks/useBoolean";

interface DropdownPanelProps {
    buttonRef: HTMLElement;
    visible: boolean;
    toggle: () => void;
    children: JSX.Element | JSX.Element[];
}

export function DropdownPanel(props: DropdownPanelProps) {
    const { visible, buttonRef, toggle, children } = props;

    const [popperElement, setPopperElement] = useState(null);

    const { styles, attributes, update } = usePopper(buttonRef, popperElement, {
        placement: "bottom-start",
    });

    const handleDocumentClick = useCallback(
        (event: any) => {
            if (!visible) {
                return;
            }

            // if we clicked inside element with 'closerClass' then hide dropdown
            if (event.target.closest("." + CSS.escape(DropdownPanel.closerClass))) {
                toggle();
                return;
            }

            if (buttonRef.contains(event.target)) {
                return;
            }
            if (popperElement.contains(event.target)) {
                return;
            }
            toggle();
        },
        [buttonRef, toggle, popperElement, visible]
    );

    useEffect(() => {
        if (visible) {
            update();
        }
    }, [visible, update]);

    useEffect(() => {
        document.addEventListener("mousedown", handleDocumentClick);
        return () => {
            document.removeEventListener("mousedown", handleDocumentClick);
        };
    }, [handleDocumentClick]);

    return (
        <div
            className={classNames("dropdown-menu", { show: visible })}
            ref={setPopperElement}
            style={styles.popper}
            {...attributes.popper}
        >
            {children}
        </div>
    );
}

DropdownPanel.closerClass = "dropdown-closer";

interface UncontrolledButtonWithDropdownPanelProps {
    buttonText: string | JSX.Element;
    children: JSX.Element | JSX.Element[];
}

export function UncontrolledButtonWithDropdownPanel(props: UncontrolledButtonWithDropdownPanelProps) {
    const { buttonText, children } = props;

    const [referenceElement, setReferenceElement] = useState(null);
    const { value: visible, toggle: togglePopper } = useBoolean(false);

    return (
        <React.Fragment>
            <Button innerRef={setReferenceElement} onClick={togglePopper} className={classNames("dropdown-toggle")}>
                {buttonText}
            </Button>

            <DropdownPanel visible={visible} toggle={togglePopper} buttonRef={referenceElement}>
                {children}
            </DropdownPanel>
        </React.Fragment>
    );
}
