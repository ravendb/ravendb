import React from "react";
import Col from "react-bootstrap/Col";
import { useOS } from "components/hooks/useOS";
import Dropdown from "react-bootstrap/Dropdown";

const KeyboardShortcuts = () => {
    const os = useOS();
    const isMac = os === "MacOS";

    return (
        <Col sm={12} className="studio-search__legend-col p-0">
            <div className="studio-search__legend-col__group">
                <Dropdown.Header className="studio-search__legend-col__group__header">
                    <div className="d-flex align-items-center gap-1">
                        <kbd>↑</kbd> <span>Move up</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>↓</kbd> <span>Move down</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>{isMac ? "⌥" : "ALT"}</kbd> <kbd>→</kbd>
                        <span>Move right</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>{isMac ? "⌥" : "ALT"}</kbd> <kbd>←</kbd>
                        <span>Move left</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>Enter</kbd> <span>Select</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>Esc</kbd> <span>Close</span>
                    </div>
                </Dropdown.Header>
            </div>
        </Col>
    );
};

export default KeyboardShortcuts;
