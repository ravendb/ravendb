import React, { ReactNode } from "react";
import { Control, FieldPath } from "react-hook-form";
import classNames from "classnames";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
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
    return (
        <div className="d-flex align-items-center gap-2" title={restriction?.tooltip}>
            <div className={classNames({ "item-disabled": !!restriction })}>
                <FormSwitch control={control} name={name} disabled={!!restriction}>
                    {children}
                </FormSwitch>
            </div>
            {restriction?.reason === "license" && (
                <LicenseRestrictedBadge licenseRequired={restriction.licenseRequired} />
            )}
        </div>
    );
}
