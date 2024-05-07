import classNames from "classnames";
import { Icon } from "components/common/Icon";
import React from "react";
import { UncontrolledDropdown, Button, DropdownToggle, DropdownMenu, DropdownItem } from "reactstrap";

interface ResetIndexesButtonProps {
    resetIndex: (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => void;
    isDropdownVisible?: boolean;
    isRounded?: boolean;
}

export default function ResetIndexesButton({ resetIndex, isDropdownVisible, isRounded }: ResetIndexesButtonProps) {
    return (
        <UncontrolledDropdown group>
            <Button
                onClick={() => resetIndex()}
                title="Reset index (rebuild)"
                color="warning"
                className={classNames({ "rounded-pill": isRounded && !isDropdownVisible })}
            >
                <Icon icon="reset-index" margin="m-0" />
            </Button>
            {isDropdownVisible && (
                <>
                    <DropdownToggle className="dropdown-toggle" color="warning" />
                    <DropdownMenu end>
                        <DropdownItem onClick={() => resetIndex("InPlace")} title="Reset index in place">
                            <Icon icon="reset-index" addon="arrow-down" />
                            Reset in place
                        </DropdownItem>
                        <DropdownItem onClick={() => resetIndex("SideBySide")} title="Reset index side by side">
                            <Icon icon="reset-index" addon="swap" />
                            Reset side by side
                        </DropdownItem>
                    </DropdownMenu>
                </>
            )}
        </UncontrolledDropdown>
    );
}
