import React, { useState, useEffect } from "react";
import { Col, DropdownItem } from "reactstrap";

const getOS = () => {
    const platform = navigator.userAgent.toLowerCase();
    if (platform.includes("mac")) return "mac";
    if (platform.includes("win")) return "windows";
    return "other";
};

const KeyboardShortcuts = () => {
    const [os, setOs] = useState(getOS());

    useEffect(() => {
        const isTesting = false; // Set this to true for testing Mac on Windows
        if (isTesting) {
            setOs("mac");
        } else {
            setOs(getOS());
        }
    }, []);

    const isMac = os === "mac";

    return (
        <Col sm={12} className="studio-search__legend-col p-0">
            <div className="studio-search__legend-col__group">
                <DropdownItem header className="studio-search__legend-col__group__header">
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
                </DropdownItem>
            </div>
        </Col>
    );
};

export default KeyboardShortcuts;
