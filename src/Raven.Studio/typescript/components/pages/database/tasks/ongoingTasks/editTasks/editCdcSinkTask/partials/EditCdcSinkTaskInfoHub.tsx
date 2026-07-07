import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export default function EditCdcSinkTaskInfoHub() {
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const hasCdcSink = useAppSelector(licenseSelectors.statusValue("HasCdcSink"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasCdcSink,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasCdcSink ? null : "licensing"}>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
                targetId="about-view"
            >
                <div>
                    A <strong>CDC Sink task</strong> consumes <strong>Change Data Capture</strong> events from tables in
                    an external relational database, including inserts, updates, and deletes. Each captured change is
                    applied as a document operation in RavenDB. Supported source databases include:
                    <br />
                    <strong>PostgreSQL, SQL Server, and MySQL / MariaDB</strong>.
                    <hr />
                    Task configuration includes:
                    <ul>
                        <li className="mt-1">
                            Provide a SQL{" "}
                            <a href={appUrl.forConnectionStrings(activeDatabaseName, "Sql")} target="_blank">
                                connection string
                            </a>{" "}
                            that points to the source database where CDC is enabled.
                        </li>
                        <li>
                            Click &quot;Discover tables&quot; under &quot;Schema Explorer&quot; to{" "}
                            <strong>fetch the available tables</strong> from the source database.
                        </li>
                        <li>
                            <strong>Configure how each source table is converted to documents:</strong>
                            <ul>
                                <li className="mt-1">
                                    <strong>Target collection</strong>
                                    <br />
                                    Set the target collection where documents generated from rows in the source table
                                    will be stored.
                                </li>
                                <li className="mt-1">
                                    <strong>Field mappings</strong>
                                    <br />
                                    Map source columns to document fields and choose how to store each value:
                                    <br />
                                    as a regular property, parsed JSON, or a RavenDB attachment.
                                </li>
                                <li className="mt-1">
                                    <strong>Document ID</strong>
                                    <br />
                                    Specify the source primary key column or columns used to uniquely identify a row.
                                    <br />
                                    Their values are used to derive the RavenDB document ID.
                                </li>
                                <li className="mt-1">
                                    <strong>Related tables</strong>
                                    <br />
                                    When a source table references another table, you can configure how the related row
                                    is represented in the RavenDB document:
                                    <ul>
                                        <li>
                                            <strong>Embedded table</strong> - include the related row’s data in the main
                                            document as an embedded object.
                                        </li>
                                        <li>
                                            <strong>Linked table</strong> - store the related row as a separate related
                                            document.
                                            <br />
                                            The related document ID is derived from the linked collection name you
                                            specify and the join column values.
                                        </li>
                                    </ul>
                                </li>
                                <li className="mt-1">
                                    <strong>Advanced: On-delete behavior</strong>
                                    <br />
                                    Choose whether deletes from the source are ignored, propagated as document
                                    deletions, or transformed by a custom patch script.
                                </li>
                                <li className="mt-1">
                                    <strong>Advanced: Patch scripts</strong>
                                    <br />
                                    You can define JavaScript snippets that customize how changes are applied before
                                    they are written to RavenDB. Use a <em>patch script</em> to transform documents on
                                    inserts and updates, and a <em>delete patch script</em> to customize what happens
                                    when source rows are deleted.
                                </li>
                            </ul>
                        </li>
                    </ul>
                </div>
                <hr />
                <p>
                    <strong>Testing:</strong>
                    <br />
                    You can test a table configuration to preview the document that will be generated from a sample row
                    before saving the task.
                </p>
                <p>
                    <strong>Initial load:</strong>
                    <br />
                    By default, the task takes a snapshot of existing rows before streaming new changes.
                    <br />
                    Enable <strong>Skip initial load</strong> to stream only new changes.
                </p>
                <p>
                    <strong>Ongoing processing:</strong>
                    <br />
                    Once the task is active, every committed change in the source tables is captured and applied to
                    RavenDB. Progress is tracked using the last processed checkpoint.
                </p>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasCdcSink} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "CDC Sink task",
        featureIcon: "sql-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
        enterpriseAi: { value: true },
    },
];
