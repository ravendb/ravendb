import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { PropsWithChildren, ReactNode, useState } from "react";
import Button from "react-bootstrap/Button";
import Popover from "react-bootstrap/Popover";

interface CellWithCopyProps extends PropsWithChildren {
    value: unknown;
    additionalButtons?: ReactNode;
}

export function CellWithCopy({ value, children, additionalButtons }: CellWithCopyProps) {
    const [valuePopover, setValuePopover] = useState<HTMLElement>();

    if (value === undefined) {
        return null;
    }

    const jsonBody = JSON.stringify(value, null, 4);

    const handleCopyToClipboard = () => {
        copyToClipboard.copy(jsonBody, "Item has been copied to clipboard");
    };

    return (
        <>
            <div ref={setValuePopover} className="table-font">
                {children}
            </div>
            <PopoverWithHover target={valuePopover} placement="bottom-start">
                <Popover.Body>
                    <pre
                        style={{ maxHeight: "300px" }}
                        className={classNames("overflow-auto rounded mb-3 p-0 token", typeof value)}
                    >
                        <Code language="json" code={jsonBody} isActionsHidden />
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
