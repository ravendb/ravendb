import React from "react";
import generalUtils from "common/generalUtils";
import Code from "components/common/Code";
import Accordion from "react-bootstrap/Accordion";
import { Icon } from "components/common/Icon";
import useUniqueId from "components/hooks/useUniqueId";

interface ConnectionTestErrorProps {
    message: string;
}

export default function ConnectionTestError({ message }: ConnectionTestErrorProps) {
    const connectionErrorAccordionId = useUniqueId("connectionErrorAccordion");

    if (!message) {
        return null;
    }

    return (
        <Accordion id={connectionErrorAccordionId} className="bs5 accordion-inside-modal" flush alwaysOpen>
            <Accordion.Item eventKey="connectionErrorContent">
                <Accordion.Header>
                    <Icon icon="danger" color="danger" className="tab-icon me-3" />
                    <div className="vstack gap-1">
                        <h4 className="m-0">Connection test failed!</h4>
                        <small className="description">{generalUtils.trimMessage(message)}</small>
                    </div>
                </Accordion.Header>
                <Accordion.Body>
                    <Code language="csharp" code={message} elementToCopy={message} />
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
    );
}
