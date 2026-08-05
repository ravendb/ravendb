import React, { ReactNode } from "react";
import { Control, FieldPath } from "react-hook-form";
import classNames from "classnames";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormSwitch } from "components/common/Form";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { ImportRestriction } from "../importRestrictions";

interface RestrictedSwitchProps {
    control: Control<ImportFromFileFormData>;
    name: FieldPath<ImportFromFileFormData>;
    restriction?: ImportRestriction;
    children: ReactNode;
}

export default function RestrictedSwitch({ control, name, restriction, children }: RestrictedSwitchProps) {
    // ConditionalPopover wraps the whole row: a disabled input swallows mouse events, so a plain
    // title attribute on it never shows - the popover listens on the wrapper instead
    return (
        <ConditionalPopover
            conditions={{ isActive: !!restriction, message: restriction?.tooltip }}
            className="align-items-center gap-2"
        >
            <div className={classNames({ "item-disabled": !!restriction })}>
                <FormSwitch control={control} name={name} disabled={!!restriction}>
                    {children}
                </FormSwitch>
            </div>
            {restriction?.reason === "license" && (
                <LicenseRestrictedBadge licenseRequired={restriction.licenseRequired} />
            )}
        </ConditionalPopover>
    );
}
