import React, { ReactNode } from "react";
import { FieldPath, FieldValues, useFormContext } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import CollapseButton from "components/common/CollapseButton";
import { FormErrorIcon } from "components/common/Form";
import useBoolean from "components/hooks/useBoolean";

interface ImportSectionProps<TFormData extends FieldValues> {
    id: string;
    title: string;
    errorPaths?: FieldPath<TFormData>[];
    children: ReactNode;
}

export default function ImportSection<TFormData extends FieldValues>({
    id,
    title,
    errorPaths,
    children,
}: ImportSectionProps<TFormData>) {
    const { value: isOpen, setValue: setIsOpen, toggle: toggleIsOpen } = useBoolean(true);
    const { control } = useFormContext<TFormData>();

    return (
        <section id={id} className="mb-5">
            <div className="d-flex align-items-center gap-2 mb-3">
                <h3 className="mb-0">{title}</h3>
                {errorPaths?.length > 0 && (
                    <FormErrorIcon control={control} paths={errorPaths} onError={() => setIsOpen(true)} />
                )}
                <CollapseButton isExpanded={isOpen} toggle={toggleIsOpen} className="p-0 text-secondary no-decor" />
            </div>
            <Collapse in={isOpen}>
                <div>{children}</div>
            </Collapse>
        </section>
    );
}
