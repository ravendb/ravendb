import React from "react";
import { AccordionItemWrapper, AccordionItemLicensing } from "./AboutView";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";

interface AccordionCommunityLicenseWarningProps {
    targetId: string;
    featureName: string;
    featureIcon: IconName;
}

export default function AccordionCommunityLicenseWarning(props: AccordionCommunityLicenseWarningProps) {
    const { targetId, featureName, featureIcon } = props;

    return (
        <AccordionItemWrapper
            icon="license"
            color="warning"
            heading="Licensing"
            description="See which plans offer this and more exciting features"
            targetId={targetId}
            pill
            pillText="Upgrade available"
            pillIcon="star-filled"
        >
            <AccordionItemLicensing
                description="This feature is not available in your license. Unleash the full potential and upgrade your plan."
                featureName={featureName}
                featureIcon={featureIcon}
                checkedLicenses={["Professional", "Enterprise"]}
            >
                <p className="lead fs-4">Get your license expanded</p>
                <div className="mb-3">
                    <a href="https://ravendb.net/contact" target="_blank" className="btn btn-primary rounded-pill">
                        <Icon icon="notifications" />
                        Contact us
                    </a>
                </div>
                <small>
                    <a href="https://ravendb.net/buy" target="_blank" className="text-muted">
                        See pricing plans
                    </a>
                </small>
            </AccordionItemLicensing>
        </AccordionItemWrapper>
    );
}
