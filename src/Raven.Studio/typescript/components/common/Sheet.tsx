import classNames from "classnames";
import {
    cloneElement,
    createContext,
    ReactElement,
    ReactNode,
    useCallback,
    useContext,
    useMemo,
    useState,
} from "react";
import useResizableWidth from "hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import "./Sheet.scss";

function useControllableState<T>({
    prop,
    defaultProp,
    onChange,
}: {
    prop?: T;
    defaultProp: T;
    onChange?: (state: T) => void;
}) {
    const [uncontrolled, setUncontrolled] = useState(defaultProp);
    const isControlled = prop !== undefined;
    const value = isControlled ? prop : uncontrolled;

    const setValue = useCallback(
        (next: T | ((prev: T) => T)) => {
            const nextValue = typeof next === "function" ? (next as (prev: T) => T)(value) : next;

            if (!isControlled) {
                setUncontrolled(nextValue);
            }
            onChange?.(nextValue);
        },
        [isControlled, value, onChange]
    );

    return [value, setValue] as const;
}

type SheetContextValue = {
    open: boolean;
    setOpen: (open: boolean) => void;
};

const SheetContext = createContext<SheetContextValue | null>(null);

function useSheetContext() {
    const ctx = useContext(SheetContext);
    if (!ctx) {
        throw new Error("Sheet subcomponent must be used within <Sheet>");
    }
    return ctx;
}

export interface SheetProps {
    open?: boolean;
    defaultOpen?: boolean;
    onOpenChange?: (open: boolean) => void;
    children: ReactNode;
}

type ClonedElement = ReactElement<{className?: string, onClick: () => void}>

export function Sheet({ open: openProp, defaultOpen = false, onOpenChange, children }: SheetProps) {
    const [open, setOpen] = useControllableState({
        prop: openProp,
        defaultProp: defaultOpen,
        onChange: onOpenChange,
    });

    const contextValue = useMemo(() => ({ open, setOpen }), [open, setOpen]);

    return <SheetContext.Provider value={contextValue}>{children}</SheetContext.Provider>;
}

export function SheetTrigger({ children }: { children: ClonedElement }) {
    const { setOpen } = useSheetContext();

    return cloneElement(children, {
        onClick: () => setOpen(true),
        className: classNames(children.props.className, "sheet-trigger"),
    });
}

export function SheetContent({ children }: { children: ReactNode }) {
    const { open } = useSheetContext();
    const [isPinned, setIsPinned] = useState(false);

    const resizable = useResizableWidth({
        initialWidth: 400,
        minWidth: 400,
        maxWidth: 600,
    });

    if (!open) {
        return null;
    }

    const positionStyle: React.CSSProperties = isPinned
        ? { position: "relative" }
        : { position: "absolute", right: 10, top: 10, bottom: 10 };

    return (
        <div
            role="dialog"
            className={classNames("sheet panel-bg-1 border-secondary vstack", {
                "h-100 border-left": isPinned,
                "border rounded-2": !isPinned,
            })}
            style={{
                ...positionStyle,
                width: `${resizable.width}px`,
                borderLeft: `1px solid ${resizable.isDragging ? "#ccc" : "#4c4c63"}`,
            }}
        >
            <ColumnResize handleMouseDown={resizable.handleMouseDown} />
            {children}
        </div>
    );
}

export function SheetClose({ children }: { children: ClonedElement }) {
    const { setOpen } = useSheetContext();

    return cloneElement(children, {
        onClick: () => setOpen(false),
    });
}
