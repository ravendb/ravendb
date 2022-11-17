import React, { useLayoutEffect, useRef } from "react";
import { FormGroup, Input } from "reactstrap";

interface CheckboxTripleProps {
    state: checkbox;
    onChanged: () => void;
    color?: string;
    title?: string;
}

export function CheckboxTriple(props: CheckboxTripleProps) {
    const { state, onChanged, color, title } = props;
    const colorClass = `form-check-${color ?? "secondary"}`;

    const inputEl = useRef<HTMLInputElement>();

    useLayoutEffect(() => {
        inputEl.current.indeterminate = state === "some_checked";
    });

    return (
        <FormGroup check className={colorClass + " form-check-lg"}>
            <Input
                type="checkbox"
                readOnly={state === "some_checked"}
                checked={state === "checked"}
                innerRef={inputEl}
                onChange={onChanged}
                title={title}
            />
        </FormGroup>
    );
}
