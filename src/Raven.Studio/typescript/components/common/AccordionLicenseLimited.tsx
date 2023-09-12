import { AccordionItemLicensing, AccordionItemWrapper } from "./AboutView";
import { Icon } from "./Icon";
import React, { ReactNode } from "react";
import IconName from "../../../typings/server/icons";
import { useRavenLink } from "components/hooks/useRavenLink";

interface AccordionLicenseLimitedProps {
    targetId: string;
    featureName: string;
    featureIcon: IconName;
    description: string | ReactNode;
    isLimited: boolean;
}

export default function AccordionLicenseLimited(props: AccordionLicenseLimitedProps) {
    const { targetId, featureName, featureIcon, description, isLimited } = props;

    const contactLink = useRavenLink({ hash: "ARVCC3", isDocs: false });
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

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
                description={isLimited ? description : null}
                featureName={featureName}
                featureIcon={featureIcon}
                checkedLicenses={["Community", "Professional", "Enterprise"]}
                isCommunityLimited
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
