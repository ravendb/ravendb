import classNames from "classnames";
import React, { MouseEvent, useCallback, useRef } from "react";

interface DropdownPanelProps {
    className?: string;
    children: JSX.Element;
}

export function DropdownPanel(props: DropdownPanelProps) {
    const { className, children } = props;
    
    const element = useRef<HTMLDivElement>();
    
    const onClick = useCallback((event: MouseEvent<HTMLElement>) => {
        const $target = $(event.target);

        const closestClosePanel = $target.closest(".close-panel");
        const clickedOnClose = !!closestClosePanel.length;
        if (clickedOnClose) {
            if (!closestClosePanel.is(":disabled")) {
                const $dropdownParent = $target.closest(".dropdown-menu").parent();
                $dropdownParent.removeClass('open');
            } else {
                event.stopPropagation();
            }
        } else {
            const $button = $target.closest(".dropdown-toggle");
            const $dropdown = $button.next(".dropdown-menu");
            if ($dropdown.length && $dropdown[0] !== element.current) {
                if (!$button.is(":disabled")) {
                    const $parent = $dropdown.parent();
                    $parent.toggleClass('open');
                }
            } else {
                // close any child dropdown
                $(".dropdown", element.current).each((idx, elem) => {
                    $(elem).removeClass('open');
                });
            }

            event.stopPropagation();
        }
    }, []);
    
    return (
        <div className={classNames("dropdown-menu", className)} onClick={onClick} ref={element}>
            {children}
        </div>
    )
}
