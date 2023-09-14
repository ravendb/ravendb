import React from "react";
import { AccordionItemWrapper, AccordionItemLicensing } from "./AboutView";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";
import { useRavenLink } from "components/hooks/useRavenLink";

interface AccordionLicenseNotIncludedProps {
    featureName: string;
    featureIcon: IconName;
    checkedLicenses: string[];
    isLimited: boolean;
}

export default function AccordionLicenseNotIncluded(props: AccordionLicenseNotIncludedProps) {
    const { featureName, featureIcon, checkedLicenses, isLimited } = props;

    const contactLink = useRavenLink({ hash: "ARVCC3", isDocs: false });
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    return (
        <AccordionItemWrapper
            icon="license"
            color={isLimited ? "warning" : "success"}
            heading="Licensing"
            description="See which plans offer this and more exciting features"
            pill={isLimited}
            pillText={isLimited ? "Upgrade available" : null}
            pillIcon={isLimited ? "upgrade-arrow" : null}
            targetId="licensing"
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
                            <a href={contactLink} target="_blank" className="btn btn-primary rounded-pill">
                                <Icon icon="notifications" />
                                Contact us
                            </a>
                        </div>
                        <small>
                            <a href={buyLink} target="_blank" className="text-muted">
                                See pricing plans
                            </a>
                        </small>
                    </>
                )}
            </AccordionItemLicensing>
        </AccordionItemWrapper>
    );
}
