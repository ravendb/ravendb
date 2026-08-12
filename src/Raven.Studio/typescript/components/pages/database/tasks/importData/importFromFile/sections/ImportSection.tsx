import React, { ReactNode } from "react";
import { useFormContext } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import CollapseButton from "components/common/CollapseButton";
import { FormErrorIcon } from "components/common/Form";
import useBoolean from "components/hooks/useBoolean";
import { ImportFromFileFormData, ImportFromFileFormPath } from "../importFromFileValidation";

interface ImportSectionProps {
    id: string;
    title: string;
    errorPaths?: ImportFromFileFormPath[];
    children: ReactNode;
}

export default function ImportSection({ id, title, errorPaths, children }: ImportSectionProps) {
    const { value: isOpen, setValue: setIsOpen, toggle: toggleIsOpen } = useBoolean(true);
    const { control } = useFormContext<ImportFromFileFormData>();

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
