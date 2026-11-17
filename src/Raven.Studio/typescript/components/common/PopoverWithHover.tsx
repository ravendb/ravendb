import { ReactNode, useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import Popover, { PopoverProps } from "react-bootstrap/Popover";
import Overlay, { OverlayProps } from "react-bootstrap/Overlay";
import { Placement, PopperRef } from "react-bootstrap/types";
import classNames from "classnames";
import { OVERLAY_MOTION_CLASS_NAME, withTopLeftPositioning } from "components/common/overlayMotion";
import useUniqueId from "components/hooks/useUniqueId";

const SHOW_DELAY_IN_MS = 180;
const PREPARE_DELAY_IN_MS = 300;

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
    openDelayInMs?: number;
}

function tooltipMutex(target: HTMLDivElement, onClose: () => void) {
    if (tooltipContext.currentTarget && tooltipContext.currentTarget !== target) {
        tooltipContext.closeAction();
    }

    tooltipContext.currentTarget = target;
    tooltipContext.closeAction = onClose;
}

export function PopoverWithHover(props: PopoverWithHoverProps) {
    const {
        target,
        children,
        placement,
        className,
        style: propsStyle,
        overlayProps,
        prepare,
        openDelayInMs,
        ...rest
    } = props;

    const div = target as HTMLDivElement;
    const [open, setOpen] = useState<boolean>(false);
    const [hasBeenOpened, setHasBeenOpened] = useState<boolean>(false);
    const overElement = useRef<boolean>(false);

    const popoverId = useUniqueId("popover-");

    const cancelHandle = useRef<ReturnType<typeof setTimeout>>(null);
    const showHandle = useRef<ReturnType<typeof setTimeout>>(null);
    const prepareHandle = useRef<ReturnType<typeof setTimeout>>(null);

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
        clearTimeout(showHandle.current);
        clearTimeout(prepareHandle.current);
        showHandle.current = null;
        prepareHandle.current = null;
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
        const opensWhenPrepared = prepare != null && openDelayInMs == null;

        const show = () => {
            setOpen(true);
            setHasBeenOpened(true);
        };

        const startPreparing = () => {
            prepareHandle.current = null;
            prepare().then(() => {
                if (opensWhenPrepared && overElement.current) {
                    show();
                }
            });
        };

        const onEnter = () => {
            overElement.current = true;
            tooltipMutex(div, () => setOpen(false));

            if (prepare) {
                prepareHandle.current = setTimeout(startPreparing, PREPARE_DELAY_IN_MS);
            }
            if (!opensWhenPrepared) {
                showHandle.current = setTimeout(show, openDelayInMs ?? SHOW_DELAY_IN_MS);
            }

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
    }, [maybeCancelShow, target, scheduleHide, maybeCancelHide, div, prepare, openDelayInMs]);

    useEffect(() => maybeCancelShow, [maybeCancelShow]);

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
                    className={classNames("bs5", OVERLAY_MOTION_CLASS_NAME, className)}
                >
                    <UpdatePlacementOnChange popper={popper}>{children}</UpdatePlacementOnChange>
                </Popover>
            )}
        </Overlay>
    );
}

function UpdatePlacementOnChange({ popper, children }: { popper: PopperRef; children: ReactNode }) {
    useLayoutEffect(() => {
        popper?.scheduleUpdate?.();
    }, [popper, children]);

    return children;
}
