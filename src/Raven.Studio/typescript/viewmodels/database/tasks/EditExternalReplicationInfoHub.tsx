import AboutViewFloating, {AboutViewAnchored, AccordionItemWrapper} from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import {Icon} from "components/common/Icon";

export function EditExternalReplicationInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

    return (
        <AboutViewFloating defaultOpen={!isProfessionalOrAbove}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Text
                </p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href="https://ravendb.net/l/MZOBO3/latest" target="_blank">
                    <Icon icon="newtab" /> Docs - External Replication
                </a>
            </AccordionItemWrapper>
            <AboutViewAnchored
                className="mt-2"
                defaultOpen={isProfessionalOrAbove ? null : "licensing"}
            >
                <AccordionLicenseNotIncluded
                    targetId="licensing"
                    featureName="External Replication"
                    featureIcon="external-replication"
                    checkedLicenses={["Professional", "Enterprise"]}
                    isLimited={!isProfessionalOrAbove}
                />
            </AboutViewAnchored>
        </AboutViewFloating>
    );
}
