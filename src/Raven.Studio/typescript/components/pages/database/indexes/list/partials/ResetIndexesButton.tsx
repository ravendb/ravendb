import classNames from "classnames";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import React from "react";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";

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
        <Dropdown>
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                variant="warning"
                className={classNames({ "rounded-pill": isRounded })}
            >
                <Icon icon="reset-index" margin="m-0" />
            </Dropdown.Toggle>
            <Dropdown.Menu>
                <Dropdown.Item onClick={() => resetIndex("InPlace")} title="Reset index in place">
                    <Icon icon="reset-index" addon="arrow-down" />
                    Reset in place
                </Dropdown.Item>
                <ConditionalPopover
                    conditions={{
                        isActive: !!sideBySideDisabledReason,
                        message: sideBySideDisabledReason,
                    }}
                >
                    <Dropdown.Item
                        onClick={() => resetIndex("SideBySide")}
                        title="Reset index side by side"
                        disabled={!!sideBySideDisabledReason}
                    >
                        <Icon icon="reset-index" addon="swap" />
                        Reset side by side
                    </Dropdown.Item>
                </ConditionalPopover>
            </Dropdown.Menu>
        </Dropdown>
    );
}
