import { createContext, ReactNode, useCallback, useContext, useEffect, useRef, useState } from "react";
import Popover, { PopoverProps } from "react-bootstrap/Popover";
import Overlay, { OverlayProps } from "react-bootstrap/Overlay";
import { Placement } from "react-bootstrap/types";
import classNames from "classnames";
import { overlayMotionClassName, withTopLeftPositioning } from "components/common/overlayMotion";
import useUniqueId from "components/hooks/useUniqueId";

const PopoverRepositionContext = createContext<() => void>(null);

export const usePopoverReposition = () => useContext(PopoverRepositionContext);

const tooltipContext = {
    currentTarget: null as HTMLDivElement,
    closeAction: null as () => void,
};

export interface PopoverWithHoverProps extends PopoverProps {
    rounded?: "true" | null;
    target: HTMLElement;
    children: ReactNode | ReactNode[];
    placement?: Placement;
    overlayProps?: Omit<OverlayProps, "target" | "show" | "placement" | "children">;
    prepare?: () => Promise<unknown>;
}

function tooltipMutex(target: HTMLDivElement, onClose: () => void) {
    if (tooltipContext.currentTarget && tooltipContext.currentTarget !== target) {
        tooltipContext.closeAction();
    }

    tooltipContext.currentTarget = target;
    tooltipContext.closeAction = onClose;
}

export function PopoverWithHover(props: PopoverWithHoverProps) {
    const { target, children, placement, className, style: propsStyle, overlayProps, prepare, ...rest } = props;

    const div = target as HTMLDivElement;
    const [open, setOpen] = useState<boolean>(false);
    const [hasBeenOpened, setHasBeenOpened] = useState<boolean>(false);
    const overElement = useRef<boolean>(false);

    const popoverId = useUniqueId("popover-");

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
        const show = () => {
            setOpen(true);
            setHasBeenOpened(true);
        };

        const onEnter = () => {
            overElement.current = true;
            tooltipMutex(div, () => setOpen(false));
            const preparation = prepare?.().catch(() => {});
            showHandle.current = setTimeout(() => {
                showHandle.current = null;
                if (!preparation) {
                    show();
                    return;
                }
                preparation.then(() => {
                    if (overElement.current) {
                        show();
                    }
                });
            }, 180);

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
    }, [maybeCancelShow, target, scheduleHide, maybeCancelHide, div, prepare]);

    if (!target || !hasBeenOpened) {
        return null;
    }

    return (
        <Overlay
            target={target}
            show={open}
            placement={placement}
            flip
            {...overlayProps}
            popperConfig={withTopLeftPositioning(overlayProps?.popperConfig)}
        >
            {({ style: overlayStyle, popper, ...props }) => (
                <Popover
                    {...props}
                    {...rest}
                    popper={popper}
                    style={{ ...overlayStyle, ...propsStyle }}
                    id={popoverId}
                    onMouseLeave={onPopoverLeave}
                    onMouseEnter={onPopoverEnter}
                    className={classNames("bs5", overlayMotionClassName, className)}
                >
                    <PopoverRepositionContext.Provider value={popper?.scheduleUpdate}>
                        {children}
                    </PopoverRepositionContext.Provider>
                </Popover>
            )}
        </Overlay>
    );
}
