import React, { ComponentProps } from "react";
import ReactSelectCreatable from "react-select/creatable";
import { GroupBase } from "react-select";
import "./Select.scss";

export default function SelectCreatable<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>,
>(props: ComponentProps<typeof ReactSelectCreatable<Option, IsMulti, Group>>) {
    return (
        <ReactSelectCreatable
            {...props}
            className="bs5 react-select-container"
            classNamePrefix="react-select"
            formatCreateLabel={(value) => value}
        />
    );
}
