import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Code, { CodeLanguage } from "components/common/Code";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CSSProperties, PropsWithChildren, ReactNode, useState } from "react";
import Button from "react-bootstrap/Button";
import Popover from "react-bootstrap/Popover";

interface CellWithCopyProps extends PropsWithChildren {
    value: unknown;
    additionalButtons?: ReactNode;
    // shows this raw text in the preview (and copies it) instead of the JSON-stringified value
    previewCode?: string;
    previewLanguage?: CodeLanguage;
    popoverMaxWidth?: string;
}

export function CellWithCopy({
    value,
    children,
    additionalButtons,
    previewCode,
    previewLanguage = "json",
    popoverMaxWidth,
}: CellWithCopyProps) {
    const [valuePopover, setValuePopover] = useState<HTMLElement>();

    if (value === undefined) {
        return null;
    }

    const previewBody = previewCode ?? JSON.stringify(value, null, 4);

    const handleCopyToClipboard = () => {
        copyToClipboard.copy(previewBody, "Item has been copied to clipboard");
    };

    return (
        <>
            <div ref={setValuePopover} className="table-font">
                {children}
            </div>
            <PopoverWithHover
                target={valuePopover}
                placement="bottom-start"
                style={popoverMaxWidth ? ({ "--bs-popover-max-width": popoverMaxWidth } as CSSProperties) : undefined}
            >
                <Popover.Body>
                    <pre
                        style={{ maxHeight: "300px" }}
                        className={classNames(
                            "overflow-auto rounded mb-3 p-0 token",
                            previewCode == null && typeof value
                        )}
                    >
                        <Code language={previewLanguage} code={previewBody} isActionsHidden />
                    </pre>
                    <span className="small-label">Actions</span>
                    <div className="d-flex gap-2">
                        <Button onClick={handleCopyToClipboard} size="sm" title="Copy to clipboard">
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                        {additionalButtons}
                    </div>
                </Popover.Body>
            </PopoverWithHover>
        </>
    );
}

export function CellWithCopyWrapper({
    getValue,
    additionalButtons,
}: {
    getValue: () => unknown;
    additionalButtons?: ReactNode;
}) {
    return (
        <CellWithCopy additionalButtons={additionalButtons} value={getValue()}>
            <CellValue value={getValue()} />
        </CellWithCopy>
    );
}
