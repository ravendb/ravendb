import React from "react";
import { RichAlert } from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";

interface AnalysisErrorProps {
    error: any;
    onReset: () => void;
}

export default function AnalysisError({ error, onReset }: AnalysisErrorProps) {
    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);

    const message = extractMessage(error);
    const details = extractDetails(error);

    return (
        <RichAlert variant="danger" title="Failed to analyze the debug package">
            <p className="mb-2">{message}</p>
            <div className="hstack gap-2 mb-2">
                <Button variant="primary" size="sm" className="rounded-pill" onClick={onReset}>
                    <Icon icon="refresh" /> Try another package
                </Button>
                {details && (
                    <Button variant="link" size="sm" onClick={toggleDetails}>
                        {detailsVisible ? "Hide" : "Show"} exception details
                    </Button>
                )}
            </div>
            {details && (
                <Collapse in={detailsVisible}>
                    <div>
                        <pre className="debug-package-exception-details mb-0">{details}</pre>
                    </div>
                </Collapse>
            )}
        </RichAlert>
    );
}

function extractMessage(error: any): string {
    if (!error) {
        return "An unknown error occurred while analyzing the debug package.";
    }

    const parsed = tryParseJson(error.responseText);
    if (parsed?.Message) {
        return parsed.Message;
    }

    return (
        error.statusText || error.message || "The package could not be analyzed. It may be corrupted or unsupported."
    );
}

function extractDetails(error: any): string {
    if (!error) {
        return null;
    }

    const parsed = tryParseJson(error.responseText);
    if (parsed?.Error) {
        return parsed.Error;
    }

    return error.responseText || null;
}

function tryParseJson(text: string): { Message?: string; Error?: string } {
    if (!text) {
        return null;
    }
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}
