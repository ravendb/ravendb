import classNames from "classnames";
import { ClassNameProps } from "components/models/common";
import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { ComponentProps, PropsWithChildren, ReactNode, useCallback } from "react";
import Button, { ButtonProps } from "react-bootstrap/Button";
import { useSplitViewContext } from "./SplitView";
import { splitViewActions, splitViewSelectors } from "components/common/splitView/store/splitViewSlice";

export type ViewSheetWidth = number | `${number}%`;

export function ViewSheet(props: ComponentProps<"div"> & Required<PropsWithChildren>) {
    const { className, children, ...rest } = props;

    return (
        <div {...rest} className={classNames("vstack h-100", className)}>
            {children}
        </div>
    );
}

interface SheetHeaderProps extends Required<PropsWithChildren>, ClassNameProps {
    isPinHidden?: boolean;
    isCloseHidden?: boolean;
}

function SheetHeader({ isCloseHidden, isPinHidden, className, children }: SheetHeaderProps) {
    const hasDefaultButtons = !isPinHidden && !isCloseHidden;

    return (
        <div
            className={classNames(
                "d-flex justify-content-between align-items-center p-2 border-bottom border-color-light panel-bg-2",
                className
            )}
        >
            {children}
            {hasDefaultButtons && (
                <div className="d-flex align-items-center">
                    {!isPinHidden && <PinButton />}
                    {!isCloseHidden && <CloseButton />}
                </div>
            )}
        </div>
    );
}

interface CloseButtonProps extends PropsWithChildren, ClassNameProps {
    variant?: ButtonProps["variant"];
}

function CloseButton(props: CloseButtonProps) {
    const { children, className = "text-reset", variant = "link" } = props;

    const { setSheetComponent } = useSplitViewContext();

    return (
        <Button variant={variant} className={className} onClick={() => setSheetComponent(null)}>
            {children ? children : <Icon icon="close" margin="m-0" />}
        </Button>
    );
}

function PinButton() {
    const dispatch = useAppDispatch();
    const isSheetPinned = useAppSelector(splitViewSelectors.isSheetPinned);

    return (
        <Button
            variant="link"
            className={classNames({ "text-reset": !isSheetPinned })}
            onClick={() => dispatch(splitViewActions.isSheetPinnedSet(!isSheetPinned))}
        >
            <Icon icon={isSheetPinned ? "pinned" : "pin"} margin="m-0" />
        </Button>
    );
}

function SheetBody(props: Required<PropsWithChildren> & ClassNameProps) {
    return <div className={classNames("p-2 flex-grow overflow-auto", props.className)}>{props.children}</div>;
}

function SheetFooter(props: Required<PropsWithChildren> & ClassNameProps) {
    return (
        <div className={classNames("p-2 border-top border-secondary panel-bg-2", props.className)}>
            {props.children}
        </div>
    );
}

export interface OpenSheetOptions {
    component: ReactNode;
    initialWidth?: ViewSheetWidth;
    minWidth?: ViewSheetWidth;
    maxWidth?: ViewSheetWidth;
    isPinned?: boolean;
}

export function useViewSheet() {
    const dispatch = useAppDispatch();
    const { setSheetComponent } = useSplitViewContext();
    const viewWidthInPx = useAppSelector(splitViewSelectors.viewWidthInPx);

    const getWidthInPx = useCallback(
        (width: ViewSheetWidth): number => {
            if (typeof width === "number") {
                return width;
            }
            return (Number(width.replace("%", "")) / 100) * viewWidthInPx;
        },
        [viewWidthInPx]
    );

    const open = (options: OpenSheetOptions) => {
        setSheetComponent(options.component);
        dispatch(splitViewActions.isSheetPinnedSet(options.isPinned ?? false));
        dispatch(splitViewActions.initialPanelWidthInPxSet(getWidthInPx(options.initialWidth ?? "50%")));
        dispatch(splitViewActions.minPanelWidthInPxSet(getWidthInPx(options.minWidth ?? "30%")));
        dispatch(splitViewActions.maxPanelWidthInPxSet(getWidthInPx(options.maxWidth ?? "75%")));
    };

    return {
        open,
        close: () => setSheetComponent(null),
    };
}

ViewSheet.Header = SheetHeader;
ViewSheet.Body = SheetBody;
ViewSheet.Footer = SheetFooter;
ViewSheet.CloseButton = CloseButton;
ViewSheet.PinButton = PinButton;
