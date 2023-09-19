import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { featureAvailabilityProfessionalOrAbove } from "components/utils/licenseLimitsUtils";

export function EditRavenEtlInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove);
    const ravenDbEtlDocsLink = useRavenLink({ hash: "GFSWLI" });

    return (
        <AboutViewFloating defaultOpen={isProfessionalOrAbove ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    A <strong>RavenDB ETL</strong> ongoing-task is an ETL (Extract, Transform & Load) process that
                    transfers data from documents from this RavenDB database to another RavenDB database.
                </p>
                <ul>
                    <li>The sent data can be filtered and modified by multiple transformation JavaScript scripts that
                        are added to the task.
                    </li>
                    <li className="margin-top-xxs">
                        Custom logic for Attachments, Counters, Time series,
                        <br/>
                        and deletion behavior can also be applied.
                        <br/>
                        Revisions are not sent by the ETL process.
                    </li>
                    <li className="margin-top-xxs">
                        Each script specifies the source collection(s) and the destination collection(s) which may be
                        different than the source.
                    </li>
                    <li className="margin-top-xxs">
                        The scripts are executed whenever documents in the source database are created, modified, or
                        deleted.
                    </li>
                    <li className="margin-top-xxs">
                        You can test each script in this view to preview the resulting documents that will be sent.
                    </li>
                </ul>
                <hr/>
                <div>
                    Task definition includes:
                    <ul>
                        <li>The transformation scripts definitions.</li>
                        <li>A connection string with URLs of the destination database servers.</li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={ravenDbEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - RavenDB ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isProfessionalOrAbove}
                data={featureAvailabilityProfessionalOrAbove}
            />
        </AboutViewFloating>
    );
}
