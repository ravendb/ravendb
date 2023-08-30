import { AccordionItemLicensing, AccordionItemWrapper } from "./AboutView";
import { Icon } from "./Icon";
import React, { ReactNode } from "react";
import IconName from "../../../typings/server/icons";

interface AccordionCommunityLicenseLimitedProps {
    targetId: string;
    featureName: string;
    featureIcon: IconName;
    description: string | ReactNode;
}

export default function AccordionCommunityLicenseLimited(props: AccordionCommunityLicenseLimitedProps) {
    const { targetId, featureName, featureIcon, description } = props;

    return (
        <AccordionItemWrapper
            icon="license"
            color="warning"
            heading="Licensing"
            description="See which plans offer this and more exciting features"
            targetId={targetId}
            pill
            pillText="Upgrade available"
            pillIcon="upgrade-arrow"
        >
            <AccordionItemLicensing
                description={description}
                featureName={featureName}
                featureIcon={featureIcon}
                checkedLicenses={["Community", "Professional", "Enterprise"]}
                isCommunityLimited
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
