import React from "react";
import generalUtils from "common/generalUtils";
import { AccordionItemWrapper } from "components/common/AboutView";
import Code from "components/common/Code";
import { UncontrolledAccordion } from "reactstrap";

interface ConnectionStringErrorProps {
    message: string;
}

export default function ConnectionStringError({ message }: ConnectionStringErrorProps) {
    if (!message) {
        return null;
    }

    return (
        <UncontrolledAccordion className="bs5 about-view-accordion" flush stayOpen>
            <AccordionItemWrapper
                heading="Connection test failed!"
                icon="danger"
                color="danger"
                description={generalUtils.trimMessage(message)}
            >
                <Code language="csharp" code={message} />
            </AccordionItemWrapper>
        </UncontrolledAccordion>
    );
}
