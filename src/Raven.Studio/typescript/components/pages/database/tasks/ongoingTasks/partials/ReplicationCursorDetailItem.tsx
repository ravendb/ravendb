import React from "react";
import Button from "react-bootstrap/Button";
import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { RichPanelDetailItem } from "components/common/RichPanel";

const maxVisibleEntries = 10;

interface ReplicationCursorDetailItemProps {
    label: string;
    cursor: string;
}

export function ReplicationCursorDetailItem({ label, cursor }: ReplicationCursorDetailItemProps) {
    if (!cursor) {
        return null;
    }

    const entries = cursor
        .split(",")
        .map((x) => x.trim())
        .filter((x) => x);

    const hiddenEntriesCount = entries.length - maxVisibleEntries;

    if (hiddenEntriesCount <= 0) {
        return <RichPanelDetailItem label={label}>{cursor}</RichPanelDetailItem>;
    }

    const handleCopyToClipboard = () => {
        copyToClipboard.copy(cursor, `${label} has been copied to clipboard`);
    };

    return (
        <RichPanelDetailItem label={label}>
            <PopoverWithHoverWrapper
                placement="top"
                message={
                    <>
                        <div className="word-break overflow-auto" style={{ maxHeight: "300px" }}>
                            {cursor}
                        </div>
                        <Button
                            onClick={handleCopyToClipboard}
                            size="sm"
                            className="mt-2"
                            title="Copy to clipboard"
                            aria-label={`Copy ${label} to clipboard`}
                        >
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                    </>
                }
            >
                {entries.slice(0, maxVisibleEntries).join(", ")}
                <span className="text-muted">{` ... (+${hiddenEntriesCount} more)`}</span>
            </PopoverWithHoverWrapper>
        </RichPanelDetailItem>
    );
}
