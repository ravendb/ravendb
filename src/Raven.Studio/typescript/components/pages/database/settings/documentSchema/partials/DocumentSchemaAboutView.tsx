import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export default function DocumentSchemaAboutView() {
    const jsonSchemaLink = "https://json-schema.org";
    const hasSchemaValidation = useAppSelector(licenseSelectors.statusValue("HasSchemaValidation"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasSchemaValidation,
            },
        ],
    });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper icon="about" color="info">
                <div>
                    <ul>
                        <li>
                            Use this view to define a <strong>JSON Schema per collection</strong> using the standard{" "}
                            <a href={jsonSchemaLink} target="_blank">
                                JSON Schema format
                            </a>
                            .
                        </li>
                        <li className="mt-2">
                            Document validation according to the defined schema is <strong>enforced</strong> in the
                            following scenarios:
                            <ul>
                                <li>
                                    Inserting documents via <i>session</i> & via <i>bulk insert</i> - invalid documents
                                    are rejected.
                                </li>
                                <li>
                                    Inserting documents via <i>patch by query</i> - the operation stops when an invalid
                                    document is encountered.
                                </li>
                                <li>Importing a database - invalid documents are skipped.</li>
                                <li>
                                    RavenDB ETL task - if the task generates documents that are invalid for the
                                    destination database, the entire batch is rejected, and the task enters a retry loop
                                    until the issue is resolved.
                                </li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            Validation is <strong>bypassed</strong> (skipped) for the following scenarios:
                            <ul>
                                <li>Internal replication</li>
                                <li>External replication</li>
                                <li>Restoring a database from backup</li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            RavenDB validates most standard JSON Schema keywords, <strong>except</strong>:
                            <ul>
                                <li>
                                    The <code>unevaluatedProperties</code> keyword is not validated.
                                </li>
                                <li>Recursive references are not supported.</li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            Once schemas are defined, you can enable or disable schema validation per collection or
                            globally for all collections from this view.
                        </li>
                        <li className="mt-2">
                            Note: You cannot revert documents to previous revisions if a validation schema is defined
                            and enabled for their collection.
                        </li>
                        <li className="mt-2">
                            To test a JSON Schema without saving it, use the <strong>Schema Playground</strong>.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasSchemaValidation} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Schema Validation",
        featureIcon: "document-schema",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
