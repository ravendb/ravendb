import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Code, { CodeLanguage } from "components/common/Code";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CSSProperties, PropsWithChildren, ReactNode, useState } from "react";
import { useAsync } from "react-async-hook";
import Button from "react-bootstrap/Button";
import Popover from "react-bootstrap/Popover";
import Spinner from "react-bootstrap/Spinner";

interface CellWithCopyProps extends PropsWithChildren {
    value: unknown;
    additionalButtons?: ReactNode;
    // shows this raw text in the preview (and copies it) instead of the JSON-stringified value
    previewCode?: string;
    previewLanguage?: CodeLanguage;
    popoverMaxWidth?: string;
    // fetches the value shown in the preview when the cell holds an incomplete one (e.g. a trimmed preview)
    resolvePreviewValue?: () => Promise<unknown>;
}

export function CellWithCopy({
    value,
    children,
    additionalButtons,
    previewCode,
    previewLanguage = "json",
    popoverMaxWidth,
    resolvePreviewValue,
}: CellWithCopyProps) {
    const [valuePopover, setValuePopover] = useState<HTMLElement>();

    if (value === undefined) {
        return null;
    }

    return (
        <>
            <div ref={setValuePopover} className="cell-preview-target">
                {children}
            </div>
            <PopoverWithHover
                target={valuePopover}
                placement="bottom-start"
                style={popoverMaxWidth ? ({ "--bs-popover-max-width": popoverMaxWidth } as CSSProperties) : undefined}
            >
                <Popover.Body>
                    {resolvePreviewValue ? (
                        <CellWithCopyResolvedPreview
                            resolvePreviewValue={resolvePreviewValue}
                            previewLanguage={previewLanguage}
                            additionalButtons={additionalButtons}
                        />
                    ) : (
                        <CellWithCopyPreview
                            value={value}
                            previewCode={previewCode}
                            previewLanguage={previewLanguage}
                            additionalButtons={additionalButtons}
                        />
                    )}
                </Popover.Body>
            </PopoverWithHover>
        </>
    );
}

function CellWithCopyPreview({
    value,
    previewCode,
    previewLanguage,
    additionalButtons,
}: Pick<CellWithCopyProps, "value" | "previewCode" | "previewLanguage" | "additionalButtons">) {
    const previewBody = previewCode ?? JSON.stringify(value, null, 4);

    const handleCopyToClipboard = () => {
        copyToClipboard.copy(previewBody, "Item has been copied to clipboard");
    };

    return (
        <>
            <pre
                style={{ maxHeight: "300px" }}
                className={classNames("overflow-auto rounded mb-3 p-0 token", previewCode == null && typeof value)}
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
        </>
    );
}

function CellWithCopyResolvedPreview({
    resolvePreviewValue,
    previewLanguage,
    additionalButtons,
}: Required<Pick<CellWithCopyProps, "resolvePreviewValue">> &
    Pick<CellWithCopyProps, "previewLanguage" | "additionalButtons">) {
    const asyncValue = useAsync(resolvePreviewValue, []);

    if (asyncValue.status === "loading" || asyncValue.status === "not-requested") {
        return <Spinner size="sm" data-testid="preview-loader" />;
    }

    if (asyncValue.status === "error") {
        return <span className="text-danger">Unable to load the value: {asyncValue.error.message}</span>;
    }

    return (
        <CellWithCopyPreview
            value={asyncValue.result}
            previewLanguage={previewLanguage}
            additionalButtons={additionalButtons}
        />
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
