import React from "react";
import Col from "react-bootstrap/Col";
import { useOS } from "components/hooks/useOS";
import Dropdown from "react-bootstrap/Dropdown";
import { Icon } from "components/common/Icon";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { useAppSelector } from "components/store";

const KeyboardShortcuts = () => {
    const os = useOS();
    const isMac = os === "MacOS";
    const isAiAssistantDisabled = useAppSelector(aiAssistantSelectors.isDisabled);

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
                    {!isAiAssistantDisabled && (
                        <div className="d-flex align-items-center gap-1 ms-auto">
                            <kbd>{isMac ? "⌘" : "Ctrl"}</kbd> <kbd>/</kbd>{" "}
                            <div className="d-flex align-items-center fs-5">
                                <Icon icon="ask-ai" className="ai-gradient" /> Ask AI
                            </div>
                        </div>
                    )}
                </Dropdown.Header>
            </div>
        </Col>
    );
};

export default KeyboardShortcuts;
