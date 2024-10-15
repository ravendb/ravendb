import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { PropsWithChildren, useState } from "react";
import { Button } from "reactstrap";

interface CellWithCopyProps extends PropsWithChildren {
    value: unknown;
}

export function CellWithCopy({ value, children }: CellWithCopyProps) {
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
            <div ref={setValuePopover}>{children}</div>
            <PopoverWithHover target={valuePopover}>
                <div className="p-2">
                    <pre
                        style={{ maxHeight: "300px" }}
                        className={classNames("overflow-auto rounded mb-3 p-0 token", typeof value)}
                    >
                        <Code language="json" code={jsonBody} />
                    </pre>
                    <span className="small-label">Actions</span>
                    <div>
                        <Button onClick={handleCopyToClipboard} color="primary" title="Copy to clipboard">
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                    </div>
                </div>
            </PopoverWithHover>
        </>
    );
}

export function CellWithCopyWrapper({ getValue }: { getValue: () => unknown }) {
    return (
        <CellWithCopy value={getValue()}>
            <CellValue value={getValue()} />
        </CellWithCopy>
    );
}
