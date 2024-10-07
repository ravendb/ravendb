import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function EditSnowflakeEtlInfoHub() {
    const hasSnowflakeEtl = useAppSelector(licenseSelectors.statusValue("HasSnowflakeEtl"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasSnowflakeEtl,
            },
        ],
    });

    const snowflakeEtlDocsLink = useRavenLink({ hash: "TODO" }); //TODO: HASH
    
    return (
        <AboutViewFloating defaultOpen={hasSnowflakeEtl ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    A <strong>Snowflake ETL</strong> ongoing-task is an ETL (Extract, Transform & Load)
                    process that transfers data from this RavenDB database to a Snowflake data warehouse.
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
                        Each script specifies the RavenDB source collection(s) and the destination Snowflake table(s).
                    </li>
                    <li className="margin-top-xxs">
                        The scripts are executed whenever documents in the source database are created, modified, or
                        deleted.
                    </li>
                    <li className="margin-top-xxs">
                        You can test each script in this view to preview the SQL statements the ETL task would send
                        the Snowflake database.
                    </li>
                </ul>
                <hr/>
                <div>
                    Task definition includes:
                    <ul>
                        <li>The transformation scripts definitions.</li>
                        <li>List of Snowflake tables the ETL task will access,
                            <br/>    
                            including the column name used for the RavenDB document ID.
                        </li>
                        <li>A connection string specifying the Snowflake storage destination,
                            <br/>
                            where "<i>Account</i>", "<i>Database</i>", and "<i>Schema</i>" are mandatory parameters.
                        </li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr/>
                <div className="small-label mb-2">useful links</div>
                <a href={snowflakeEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Snowflake ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasSnowflakeEtl}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Snowflake ETL",
        featureIcon: "snowflake-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
