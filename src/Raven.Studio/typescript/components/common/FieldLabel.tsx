import { ReactNode } from "react";
import { FormLabelOwnProps } from "react-bootstrap/FormLabel";
import { FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { PopoverWithHoverProps } from "components/common/PopoverWithHover";

interface FieldLabelProps extends FormLabelOwnProps {
    tooltip?: ReactNode;
    tooltipPlacement?: PopoverWithHoverProps["placement"];
}

export default function FieldLabel({ tooltip, tooltipPlacement, children, ...rest }: FieldLabelProps) {
    return (
        <FormLabel {...rest}>
            {children}
            {tooltip && (
                <PopoverWithHoverWrapper message={tooltip} placement={tooltipPlacement}>
                    <Icon icon="info-new" margin="ms-1" />
                </PopoverWithHoverWrapper>
            )}
        </FormLabel>
    );
}
