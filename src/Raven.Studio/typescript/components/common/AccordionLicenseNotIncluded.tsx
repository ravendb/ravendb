import React from "react";
import { AccordionItemWrapper, AccordionItemLicensing } from "./AboutView";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";

interface AccordionLicenseNotIncludedProps {
    targetId: string;
    featureName: string;
    featureIcon: IconName;
    checkedLicenses: string[];
    isLimited: boolean;
}

export default function AccordionLicenseNotIncluded(props: AccordionLicenseNotIncludedProps) {
    const { targetId, featureName, featureIcon, checkedLicenses, isLimited } = props;

    return (
        <AccordionItemWrapper
            icon="license"
            color={isLimited ? "warning" : "success"}
            heading="Licensing"
            description="See which plans offer this and more exciting features"
            targetId={targetId}
            pill={isLimited}
            pillText={isLimited ? "Upgrade available" : null}
            pillIcon={isLimited ? "upgrade-arrow" : null}
        >
            <AccordionItemLicensing
                description={
                    isLimited
                        ? "This feature is not available in your license. Unleash the full potential and upgrade your plan."
                        : null
                }
                featureName={featureName}
                featureIcon={featureIcon}
                checkedLicenses={checkedLicenses}
            >
                {isLimited && (
                    <>
                        <p className="lead fs-4">Get your license expanded</p>
                        <div className="mb-3">
                            <a
                                href="https://ravendb.net/contact"
                                target="_blank"
                                className="btn btn-primary rounded-pill"
                            >
                                <Icon icon="notifications" />
                                Contact us
                            </a>
                        </div>
                        <small>
                            <a href="https://ravendb.net/buy" target="_blank" className="text-muted">
                                See pricing plans
                            </a>
                        </small>
                    </>
                )}
            </AccordionItemLicensing>
        </AccordionItemWrapper>
    );
}
