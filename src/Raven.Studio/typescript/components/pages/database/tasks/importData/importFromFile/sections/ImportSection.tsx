import React, { ReactNode, useState } from "react";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";

interface ImportSectionProps {
    id: string;
    title: string;
    children: ReactNode;
}

export default function ImportSection({ id, title, children }: ImportSectionProps) {
    const [isOpen, setIsOpen] = useState(true);

    return (
        <section id={id} className="mb-5">
            <div className="d-flex align-items-center gap-2 mb-3">
                <h3 className="mb-0">{title}</h3>
                <button
                    type="button"
                    className="btn btn-link p-0 text-secondary no-decor"
                    aria-expanded={isOpen}
                    title={isOpen ? "Collapse section" : "Expand section"}
                    onClick={() => setIsOpen(!isOpen)}
                >
                    <Icon icon={isOpen ? "collapse-vertical" : "expand-vertical"} margin="m-0" />
                </button>
            </div>
            <Collapse in={isOpen}>
                <div>{children}</div>
            </Collapse>
        </section>
    );
}
