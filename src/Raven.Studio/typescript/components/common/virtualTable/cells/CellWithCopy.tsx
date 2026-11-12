import "./CellWithCopy.scss";
import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Code, { CodeLanguage } from "components/common/Code";
import { Icon } from "components/common/Icon";
import { overlayPendingClassName } from "components/common/overlayMotion";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CSSProperties, PropsWithChildren, ReactNode, useState } from "react";
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
    resolvePreviewValue?: () => Promise<unknown>;
}

type ResolvedPreview = { value: unknown } | { errorMessage: string };

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
    const [resolvedPreview, setResolvedPreview] = useState<ResolvedPreview>();

    if (value === undefined) {
        return null;
    }

    const resolvePreview = resolvePreviewValue
        ? async () => {
              try {
                  setResolvedPreview({ value: await resolvePreviewValue() });
              } catch (error) {
                  setResolvedPreview({ errorMessage: error instanceof Error ? error.message : String(error) });
              }
          }
        : undefined;

    return (
        <>
            <div ref={setValuePopover} className="cell-preview-target">
                {children}
            </div>
            <PopoverWithHover
                target={valuePopover}
                placement="bottom-start"
                prepare={resolvePreview}
                style={popoverMaxWidth ? ({ "--bs-popover-max-width": popoverMaxWidth } as CSSProperties) : undefined}
            >
                <Popover.Body>
                    {resolvePreviewValue ? (
                        <CellWithCopyResolvedPreview
                            resolvedPreview={resolvedPreview}
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
                className={classNames(
                    "cell-preview-code overflow-auto rounded mb-3 p-0 token",
                    previewCode == null && typeof value
                )}
            >
                <Code language={previewLanguage} code={previewBody} isActionsHidden />
            </pre>
            <span className="small-label">Actions</span>
            <div className="d-flex gap-2">
                <Button onClick={handleCopyToClipboard} size="sm" title="Copy to clipboard">
                    <Icon icon="copy" margin="m-0" />
                </Button>
                {additionalButtons}
            </div>
        </>
    );
}

function CellWithCopyResolvedPreview({
    resolvedPreview,
    previewLanguage,
    additionalButtons,
}: { resolvedPreview?: ResolvedPreview } & Pick<CellWithCopyProps, "previewLanguage" | "additionalButtons">) {
    if (!resolvedPreview) {
        return <Spinner size="sm" className={overlayPendingClassName} />;
    }

    if ("errorMessage" in resolvedPreview) {
        return <span className="text-danger">Unable to load the value: {resolvedPreview.errorMessage}</span>;
    }

    if (resolvedPreview.value === undefined) {
        return <span className="text-muted">The value no longer exists</span>;
    }

    return (
        <CellWithCopyPreview
            value={resolvedPreview.value}
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
