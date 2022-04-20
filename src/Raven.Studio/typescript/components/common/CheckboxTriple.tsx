import React, { useLayoutEffect, useRef } from "react";

interface CheckboxTripleProps {
    state: checkbox;
    onChanged: () => void;
}

export function CheckboxTriple(props: CheckboxTripleProps) {
    const { state, onChanged } = props;

    const inputEl = useRef<HTMLInputElement>();

    useLayoutEffect(() => {
        inputEl.current.indeterminate = state === "some_checked";
    });

    return (
        <input
            type="checkbox"
            className="styled"
            readOnly={state === "some_checked"}
            checked={state === "checked"}
            ref={inputEl}
            onChange={onChanged}
        />
    );
}
