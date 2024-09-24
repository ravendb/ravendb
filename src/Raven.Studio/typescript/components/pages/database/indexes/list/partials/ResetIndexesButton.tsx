import classNames from "classnames";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import React from "react";
import { UncontrolledDropdown, DropdownToggle, DropdownMenu, DropdownItem } from "reactstrap";

interface ResetIndexesButtonProps {
    resetIndex: (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => void;
    isRounded?: boolean;
    sideBySideDisabledReason?: string;
}

export default function ResetIndexesButton({
    resetIndex,
    isRounded,
    sideBySideDisabledReason,
}: ResetIndexesButtonProps) {
    return (
        <UncontrolledDropdown group>
            <DropdownToggle caret color="warning" className={classNames({ "rounded-pill": isRounded })}>
                <Icon icon="reset-index" margin="m-0" />
            </DropdownToggle>
            <DropdownMenu end>
                <DropdownItem onClick={() => resetIndex("InPlace")} title="Reset index in place">
                    <Icon icon="reset-index" addon="arrow-down" />
                    Reset in place
                </DropdownItem>
                <ConditionalPopover
                    conditions={{
                        isActive: !!sideBySideDisabledReason,
                        message: sideBySideDisabledReason,
                    }}
                >
                    <DropdownItem
                        onClick={() => resetIndex("SideBySide")}
                        title="Reset index side by side"
                        disabled={!!sideBySideDisabledReason}
                    >
                        <Icon icon="reset-index" addon="swap" />
                        Reset side by side
                    </DropdownItem>
                </ConditionalPopover>
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}
