import React, { ReactNode } from "react";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";

interface ImportSectionProps {
    id: string;
    title: string;
    children: ReactNode;
}

export default function ImportSection({ id, title, children }: ImportSectionProps) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(true);

    return (
        <section id={id} className="mb-5">
            <div className="d-flex align-items-center gap-2 mb-3">
                <h3 className="mb-0">{title}</h3>
                <Button
                    variant="link"
                    className="p-0 text-secondary no-decor"
                    aria-expanded={isOpen}
                    title={isOpen ? "Collapse section" : "Expand section"}
                    onClick={toggleIsOpen}
                >
                    <Icon icon={isOpen ? "collapse-vertical" : "expand-vertical"} margin="m-0" />
                </Button>
            </div>
            <Collapse in={isOpen}>
                <div>{children}</div>
            </Collapse>
        </section>
    );
}
