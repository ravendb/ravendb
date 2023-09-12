import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import {Icon} from "components/common/Icon";

export function EditReplicationSinkInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

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
                    Text
                </p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href="https://ravendb.net/l/MMUKGD/latest" target="_blank">
                    <Icon icon="newtab" /> Docs - Replication Sink
                </a>
            </AccordionItemWrapper>
            <AccordionLicenseNotIncluded
                targetId="licensing"
                featureName="Replication Sink"
                featureIcon="pull-replication-agent"
                checkedLicenses={["Professional", "Enterprise"]}
                isLimited={!isProfessionalOrAbove}
            />
        </AboutViewFloating>
    );
}
