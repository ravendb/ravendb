import React, { ReactNode } from "react";
import { Control } from "react-hook-form";
import classNames from "classnames";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { FormSwitch } from "components/common/Form";
import {
    ImportOptionsFormData,
    ImportOptionsFormPath,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import { ImportRestriction } from "components/pages/database/tasks/importData/shared/importRestrictions";

interface RestrictedSwitchProps {
    control: Control<ImportOptionsFormData>;
    name: ImportOptionsFormPath;
    restriction?: ImportRestriction;
    warning?: ReactNode;
    afterChange?: (value: boolean) => void;
    className?: string;
    children: ReactNode;
}

export default function RestrictedSwitch({
    control,
    name,
    restriction,
    warning,
    afterChange,
    className,
    children,
}: RestrictedSwitchProps) {
    return (
        <div className="d-flex align-items-center gap-2">
            <ConditionalPopover conditions={{ isActive: !!restriction, message: restriction?.tooltip }}>
                <div className={classNames({ "item-disabled": !!restriction })}>
                    <FormSwitch
                        control={control}
                        name={name}
                        disabled={!!restriction}
                        className={className}
                        afterChange={afterChange}
                    >
                        {children}
                    </FormSwitch>
                </div>
            </ConditionalPopover>
            {warning && (
                <PopoverWithHoverWrapper message={warning}>
                    <Icon icon="warning" color="warning" margin="m-0" data-testid="restricted-switch-warning" />
                </PopoverWithHoverWrapper>
            )}
            {restriction?.reason === "license" && (
                <LicenseRestrictedBadge licenseRequired={restriction.licenseRequired} />
            )}
        </div>
    );
}
