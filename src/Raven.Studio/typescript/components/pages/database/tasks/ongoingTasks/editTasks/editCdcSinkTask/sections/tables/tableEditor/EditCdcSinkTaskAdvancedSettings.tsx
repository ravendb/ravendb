import { ReactNode } from "react";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";

interface EditCdcSinkTaskAdvancedSettingsProps {
    hasAdvancedValues: boolean;
    children: ReactNode;
}

export default function EditCdcSinkTaskAdvancedSettings({
    hasAdvancedValues,
    children,
}: EditCdcSinkTaskAdvancedSettingsProps) {
    return (
        <Accordion defaultActiveKey={hasAdvancedValues ? "advanced-settings" : null} className="mt-2">
            <Accordion.Item eventKey="advanced-settings" className="border border-secondary rounded-2 panel-bg-2">
                <Accordion.Header
                    as={() => (
                        <AccordionButton className="rounded-2 panel-bg-2 fs-5 p-1">Advanced settings</AccordionButton>
                    )}
                ></Accordion.Header>
                <Accordion.Body className="p-2">
                    <div className="vstack gap-2">{children}</div>
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
    );
}
