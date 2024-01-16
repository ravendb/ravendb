import React from "react";
import generalUtils from "common/generalUtils";
import Code from "components/common/Code";
import { AccordionBody, AccordionHeader, AccordionItem, UncontrolledAccordion } from "reactstrap";
import { Icon } from "components/common/Icon";
import useId from "hooks/useId";

interface ConnectionTestErrorProps {
    message: string;
}

export default function ConnectionTestError({ message }: ConnectionTestErrorProps) {
    const connectionErrorAccordionId = useId("connectionErrorAccordion");

    if (!message) {
        return null;
    }

    return (
        <UncontrolledAccordion
            id={connectionErrorAccordionId}
            toggle={null}
            className="bs5 accordion-inside-modal"
            flush
            stayOpen
        >
            <AccordionItem>
                <AccordionHeader targetId="connectionErrorContent">
                    <Icon icon="danger" color="danger" className="tab-icon me-3" />
                    <div className="vstack gap-1">
                        <h4 className="m-0">Connection test failed!</h4>
                        <small className="description">{generalUtils.trimMessage(message)}</small>
                    </div>
                </AccordionHeader>
                <AccordionBody accordionId="connectionErrorContent">
                    <Code language="csharp" code={message} elementToCopy={message} />
                </AccordionBody>
            </AccordionItem>
        </UncontrolledAccordion>
    );
}
