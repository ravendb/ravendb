import React, { ReactNode } from "react";
import { Control, FieldPath } from "react-hook-form";
import classNames from "classnames";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { FormSwitch } from "components/common/Form";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { ImportRestriction } from "../importRestrictions";

interface RestrictedSwitchProps {
    control: Control<ImportFromFileFormData>;
    name: FieldPath<ImportFromFileFormData>;
    restriction?: ImportRestriction;
    /** Shown as a warning icon next to the row - the switch stays usable. */
    warning?: ReactNode;
    children: ReactNode;
}

export default function RestrictedSwitch({ control, name, restriction, warning, children }: RestrictedSwitchProps) {
    return (
        <div className="d-flex align-items-center gap-2">
            <ConditionalPopover conditions={{ isActive: !!restriction, message: restriction?.tooltip }}>
                <div className={classNames({ "item-disabled": !!restriction })}>
                    <FormSwitch control={control} name={name} disabled={!!restriction}>
                        {children}
                    </FormSwitch>
                </div>
            </ConditionalPopover>
            {warning && (
                <PopoverWithHoverWrapper message={warning}>
                    <Icon icon="warning" color="warning" margin="m-0" />
                </PopoverWithHoverWrapper>
            )}
            {restriction?.reason === "license" && (
                <LicenseRestrictedBadge licenseRequired={restriction.licenseRequired} />
            )}
        </div>
    );
}
