import { CellContext } from "@tanstack/react-table";
import copyToClipboard from "common/copyToClipboard";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import React from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

type CellContextSubset<TData, TValue> = Pick<CellContext<TData, TValue>, "cell" | "getValue">;

export interface CellWithDescriptionProps<TData, TValue> extends CellContextSubset<TData, TValue> {
    description?: string;
    isActionsMenuVisible?: boolean;
    cellClassName?: string;
}

export function CellWithDescription<TData, TValue>({
    cell,
    getValue,
    description = String(getValue()),
    isActionsMenuVisible = true,
    cellClassName,
}: CellWithDescriptionProps<TData, TValue>) {
    const handleCopyToClipboard = () => {
        copyToClipboard.copy(description, "Item has been copied to clipboard");
    };

    return (
        <PopoverWithHoverWrapper
            message={
                <div className="p-1">
                    <div>{description}</div>
                    {isActionsMenuVisible && (
                        <>
                            <span className="small-label">Actions</span>
                            <div>
                                <Button onClick={handleCopyToClipboard} variant="primary" title="Copy to clipboard">
                                    <Icon icon="copy-to-clipboard" margin="m-0" />
                                </Button>
                            </div>
                        </>
                    )}
                </div>
            }
        >
            <CellValueWrapper className={cellClassName} id={`popover-${cell.id}`} getValue={getValue} />
        </PopoverWithHoverWrapper>
    );
}
