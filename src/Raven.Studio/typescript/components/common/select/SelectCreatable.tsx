import { ComponentProps } from "react";
import ReactSelectCreatable from "react-select/creatable";
import { GroupBase } from "react-select";
import "./Select.scss";
import classNames from "classnames";
import { applyRoundedPillStyle } from "components/common/select/Select";

interface SelectCreatableProps<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>,
> extends ComponentProps<typeof ReactSelectCreatable<Option, IsMulti, Group>> {
    isRoundedPill?: boolean;
    isClearedAfterSelect?: boolean;
}

export default function SelectCreatable<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>,
>({
    isRoundedPill,
    isClearedAfterSelect,
    className,
    styles = {},
    classNames: classNamesProp,
    ...rest
}: SelectCreatableProps<Option, IsMulti, Group>) {
    if (isRoundedPill) {
        applyRoundedPillStyle(styles);
    }

    return (
        <ReactSelectCreatable
            styles={styles}
            formatCreateLabel={(value) => value ?? ""}
            menuPortalTarget={document.body}
            {...rest}
            classNamePrefix="react-select"
            className={classNames("bs5 react-select-container", { "rounded-pill": isRoundedPill }, className)}
            classNames={{ menuPortal: () => "bs5 react-select-container", ...classNamesProp }}
            value={isClearedAfterSelect ? null : rest.value}
        />
    );
}
