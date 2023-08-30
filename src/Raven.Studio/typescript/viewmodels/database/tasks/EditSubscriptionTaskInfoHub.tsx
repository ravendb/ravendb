import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionCommunityLicenseLimited from "components/common/AccordionCommunityLicenseLimited";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";

export function EditSubscriptionTaskInfoHub() {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    This is subscription task edit view
                </p>
            </AccordionItemWrapper>
            {licenseType === "Community" && (
                <AccordionCommunityLicenseLimited
                    description="Your Community license does not include Subscriptions Revisions. Upgrade to a paid plan and get unlimited availability."
                    targetId="licensing"
                    featureName="Subscriptions"
                    featureIcon="subscription"
                />
            )}
        </AboutViewFloating>
    );
}
