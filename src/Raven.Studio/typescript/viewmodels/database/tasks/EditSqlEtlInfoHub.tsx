import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { useProfessionalOrAboveLicenseAvailability } from "components/utils/licenseLimitsUtils";

export function EditSqlEtlInfoHub() {
    const isFeatureInLicense = useAppSelector(licenseSelectors.statusValue("HasSqlEtl"));
    const featureAvailability = useProfessionalOrAboveLicenseAvailability(isFeatureInLicense);

    const sqlEtlDocsLink = useRavenLink({ hash: "7J6SEO" });

    return (
        <AboutViewFloating defaultOpen={isFeatureInLicense ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    A <strong>SQL ETL</strong> ongoing-task is an ETL (Extract, Transform & Load) process
                    that transfers data from this RavenDB database to a relational database.
                </p>
                <ul>
                    <li>
                        Data is extracted from documents & attachments only.
                        <br/>
                        Counters, Time series, and Revisions are not sent.
                    </li>
                    <li className="margin-top-xxs">
                        The sent data can be filtered and modified by multiple transformation JavaScript scripts that
                        are added to the task.
                    </li>
                    <li className="margin-top-xxs">
                        Each script specifies the RavenDB source collection(s) and the destination SQL table(s).
                    </li>
                    <li className="margin-top-xxs">
                        The scripts are executed whenever documents in the source database are created, modified, or
                        deleted.
                    </li>
                    <li className="margin-top-xxs">
                        You can test each script in this view to preview the SQL statements the ETL task would send
                        the relational database.
                    </li>
                </ul>
                <hr/>
                <div>
                    Task definition includes:
                    <ul>
                        <li>The transformation scripts definitions.</li>
                        <li>List of SQL tables the ETL task will access, including the column name used for the RavenDB
                            document ID.
                        </li>
                        <li>The destination RDBMS factory name and its corresponding connection string.
                        </li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={sqlEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - SQL ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isFeatureInLicense}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}
