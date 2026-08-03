import "./SplitView.scss";
import {
    createContext,
    CSSProperties,
    PropsWithChildren,
    ReactNode,
    useCallback,
    useContext,
    useEffect,
    useMemo,
    useRef,
    useState,
} from "react";
import ColumnResize from "../ColumnResize";
import useResizableWidth from "components/hooks/useResizableWidth";
import classNames from "classnames";
import SizeGetter from "../SizeGetter";
import { ClassNameProps } from "components/models/common";
import { useAppDispatch, useAppSelector } from "components/store";
import { splitViewActions, splitViewSelectors } from "components/common/splitView/store/splitViewSlice";

const SplitViewContext = createContext<{
    sheetComponent: ReactNode;
    setSheetComponent: (sheetComponent: ReactNode) => void;
    registerSheetClose: (callback: (() => void) | null) => void;
    activeSheetOwnerId: string | null;
    setActiveSheetOwnerId: (ownerId: string | null) => void;
    sheetPortalNode: HTMLDivElement | null;
    setSheetPortalNode: (node: HTMLDivElement | null) => void;
}>(null);

export const useSplitViewContext = () => useContext(SplitViewContext);

export function SplitViewProvider(props: Required<PropsWithChildren>) {
    return <SizeGetter render={({ width }) => <SplitViewWithSize viewWidthInPx={width} {...props} />} />;
}

function SplitViewWithSize(props: Required<PropsWithChildren> & { viewWidthInPx: number }) {
    const dispatch = useAppDispatch();
    const [sheetComponent, setSheetComponentState] = useState<ReactNode>(null);
    const [activeSheetOwnerId, setActiveSheetOwnerId] = useState<string | null>(null);
    const [sheetPortalNode, setSheetPortalNode] = useState<HTMLDivElement | null>(null);
    const onSheetCloseRef = useRef<(() => void) | null>(null);

    const setSheetComponent = useCallback((component: ReactNode) => {
        if (component === null) {
            onSheetCloseRef.current?.();
            onSheetCloseRef.current = null;
            setActiveSheetOwnerId(null);
        }
        setSheetComponentState(component);
    }, []);

    const registerSheetClose = useCallback((callback: (() => void) | null) => {
        onSheetCloseRef.current = callback;
    }, []);

    useEffect(() => {
        dispatch(splitViewActions.viewWidthInPxSet(props.viewWidthInPx));
    }, [props.viewWidthInPx]);

    const contextValue = useMemo(
        () => ({
            sheetComponent,
            setSheetComponent,
            registerSheetClose,
            activeSheetOwnerId,
            setActiveSheetOwnerId,
            sheetPortalNode,
            setSheetPortalNode,
        }),
        [sheetComponent, setSheetComponent, registerSheetClose, activeSheetOwnerId, sheetPortalNode]
    );

    return (
        <SplitViewContext.Provider value={contextValue}>
            <div className="split-view">
                <BodyWrapper>{props.children}</BodyWrapper>
                <SheetWrapper>{sheetComponent}</SheetWrapper>
            </div>
        </SplitViewContext.Provider>
    );
}

function BodyWrapper(props: Required<PropsWithChildren> & ClassNameProps) {
    const viewWidthInPx = useAppSelector(splitViewSelectors.viewWidthInPx);
    const isSheetPinned = useAppSelector(splitViewSelectors.isSheetPinned);
    const sheetWidthInPx = useAppSelector(splitViewSelectors.sheetWidthInPx);

    const width = isSheetPinned ? viewWidthInPx - sheetWidthInPx : "100%";

    return (
        <div className={classNames("split-view-body", props.className)} style={{ width }}>
            {props.children}
        </div>
    );
}

function SheetWrapper(props: Required<PropsWithChildren> & ClassNameProps) {
    const dispatch = useAppDispatch();
    const initialPanelWidthInPx = useAppSelector(splitViewSelectors.initialPanelWidthInPx);
    const minPanelWidthInPx = useAppSelector(splitViewSelectors.minPanelWidthInPx);
    const maxPanelWidthInPx = useAppSelector(splitViewSelectors.maxPanelWidthInPx);
    const isSheetPinned = useAppSelector(splitViewSelectors.isSheetPinned);

    const resizable = useResizableWidth({
        initialWidth: initialPanelWidthInPx,
        minWidth: minPanelWidthInPx,
        maxWidth: maxPanelWidthInPx,
    });

    useEffect(() => {
        dispatch(splitViewActions.sheetWidthInPxSet(resizable.width));
    }, [resizable.width]);

    if (!props.children) {
        return null;
    }

    const positionStyle: CSSProperties = isSheetPinned
        ? { position: "relative" }
        : { position: "absolute", right: 0, top: 0, bottom: 0, zIndex: "var(--zindex-unpinned-split-view)" };

    return (
        <div
            className={classNames("split-view-panel", { "is-dragging": resizable.isDragging }, props.className)}
            style={{
                ...positionStyle,
                width: resizable.width,
            }}
        >
            <ColumnResize handleMouseDown={resizable.handleMouseDown} />
            {props.children}
        </div>
    );
}
