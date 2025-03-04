import React, { useEffect, useRef } from "react";
import Form from "react-bootstrap/Form";

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

    useEffect(() => {
        inputEl.current.indeterminate = state === "some_checked";
    }, [state]);

    return (
        <Form.Group className={colorClass + " form-check-lg"}>
            <Form.Check
                type="checkbox"
                readOnly={state === "some_checked"}
                checked={state === "checked"}
                ref={inputEl}
                onChange={onChanged}
                title={title}
            />
        </Form.Group>
    );
}
