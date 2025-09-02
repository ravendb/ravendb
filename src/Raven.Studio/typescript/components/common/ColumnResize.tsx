interface ColumnResizeProps {
    handleMouseDown: (e: React.MouseEvent) => void;
}

export default function ColumnResize({ handleMouseDown }: ColumnResizeProps) {
    return (
        <div
            style={{
                position: "absolute",
                left: "-5px",
                top: 0,
                bottom: 0,
                width: "10px",
                cursor: "col-resize",
            }}
            onMouseDown={handleMouseDown}
        />
    );
}
