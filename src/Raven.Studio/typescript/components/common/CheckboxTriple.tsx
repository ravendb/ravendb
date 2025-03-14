import React, { useEffect, useRef } from "react";
import Form from "react-bootstrap/Form";
import { FormGroup } from "components/common/Form";

interface CheckboxTripleProps {
    state: checkbox;
    onChanged: () => void;
    color?: string;
    title?: string;
}

export function CheckboxTriple(props: CheckboxTripleProps) {
    const { state, onChanged, color, title } = props;
    const colorClass = `form-check-${color ?? "secondary"}`;

    const inputEl = useRef<HTMLInputElement>(null);

    useEffect(() => {
        inputEl.current.indeterminate = state === "some_checked";
    }, [state]);

    return (
        <FormGroup className={colorClass + " form-check-lg"}>
            <Form.Check
                type="checkbox"
                readOnly={state === "some_checked"}
                checked={state === "checked"}
                ref={inputEl}
                onChange={onChanged}
                title={title}
            />
        </FormGroup>
    );
}
