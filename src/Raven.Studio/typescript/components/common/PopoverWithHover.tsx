import React, { useCallback, useEffect, useRef, useState } from "react";
import { Popover } from "reactstrap";
import { PopoverProps } from "reactstrap/types/lib/Popover";

const tooltipContext = {
    currentTarget: null as HTMLDivElement,
    closeAction: null as () => void,
};

interface PopoverWithHoverProps extends PopoverProps {
    rounded?: "true" | null;
    target: HTMLElement;
    children: JSX.Element | JSX.Element[];
}

function tooltipMutex(target: HTMLDivElement, onClose: () => void) {
    if (tooltipContext.currentTarget && tooltipContext.currentTarget !== target) {
        tooltipContext.closeAction();
    }

    tooltipContext.currentTarget = target;
    tooltipContext.closeAction = onClose;
}

export function PopoverWithHover(props: PopoverWithHoverProps) {
    const { target, children, ...rest } = props;

    const div = target as HTMLDivElement;
    const [open, setOpen] = useState<boolean>(false);
    const overElement = useRef<boolean>(false);

    const cancelHandle = useRef<ReturnType<typeof setTimeout>>(null);
    const showHandle = useRef<ReturnType<typeof setTimeout>>(null);

    const scheduleHide = useCallback(() => {
        cancelHandle.current = setTimeout(() => {
            if (!overElement.current) {
                setOpen(false);
                if (tooltipContext.currentTarget === div) {
                    tooltipContext.currentTarget = null;
                    tooltipContext.closeAction = null;
                }
            }
        }, 300);
    }, [overElement, div]);

    const maybeCancelHide = useCallback(() => {
        if (cancelHandle.current) {
            clearTimeout(cancelHandle.current);
            cancelHandle.current = null;
        }
    }, []);

    const maybeCancelShow = useCallback(() => {
        if (showHandle.current) {
            clearTimeout(showHandle.current);
            showHandle.current = null;
        }
    }, []);

    const onPopoverEnter = useCallback(() => {
        setTimeout(() => {
            overElement.current = true;
            maybeCancelHide();
        }, 0);
    }, [maybeCancelHide]);

    const onPopoverLeave = useCallback(() => {
        overElement.current = false;
        scheduleHide();
    }, [scheduleHide]);

    useEffect(() => {
        const onEnter = () => {
            overElement.current = true;
            tooltipMutex(div, () => setOpen(false));
            showHandle.current = setTimeout(() => {
                setOpen(true);
                showHandle.current = null;
            }, 200);

            maybeCancelHide();
        };

        const onLeave = () => {
            maybeCancelShow();
            overElement.current = false;
            scheduleHide();
        };

        if (div) {
            div.addEventListener("mouseenter", onEnter);
            div.addEventListener("mouseleave", onLeave);

            return () => {
                div.removeEventListener("mouseenter", onEnter);
                div.removeEventListener("mouseleave", onLeave);
            };
        }
    }, [maybeCancelShow, target, scheduleHide, maybeCancelHide, div]);

    if (!target) {
        return null;
    }

    return (
        <Popover target={target} popperClassName="bs5" onMouseEnter={onPopoverEnter} isOpen={open} {...rest}>
            <div onMouseLeave={onPopoverLeave}>{children}</div>
        </Popover>
    );
}
