interface ColumnResizeProps {
    handleMouseDown: (e: React.MouseEvent) => void;
    placement?: "left" | "right";
}

export default function ColumnResize({ handleMouseDown, placement = "left" }: ColumnResizeProps) {
    return (
        <div
            style={{
                position: "absolute",
                left: placement === "left" ? "-5px" : undefined,
                right: placement === "right" ? "-5px" : undefined,
                top: 0,
                bottom: 0,
                width: "10px",
                cursor: "col-resize",
            }}
            onMouseDown={handleMouseDown}
        />
    );
}
