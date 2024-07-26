import React, { useState } from "react";
import { Col, Row, Card, Collapse, Form, Alert } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormSwitch } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import { useRavenLink } from "components/hooks/useRavenLink";
import classNames from "classnames";
import { tryHandleSubmit } from "components/utils/common";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import FeatureNotAvailableInYourLicensePopover from "components/common/FeatureNotAvailableInYourLicensePopover";
import DocumentsCompressionConfiguration = Raven.Client.ServerWide.DocumentsCompressionConfiguration;
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { SelectOption } from "components/common/select/Select";
import { useAppUrls } from "components/hooks/useAppUrls";
import FormCollectionsSelect from "components/common/FormCollectionsSelect";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

export default function DocumentCompression() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { databasesService } = useServices();

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );

    const [customCollectionOptions, setCustomCollectionOptions] = useState<SelectOption[]>([]);

    const asyncGetConfig = useAsyncCallback(() => databasesService.getDocumentsCompressionConfiguration(databaseName), {
        onSuccess: (result) => {
            setCustomCollectionOptions(
                result.Collections.filter((x) => !allCollectionNames.includes(x)).map((x) => ({
                    value: x,
                    label: x,
                }))
            );
        },
    });

    const { formState, control, setValue, reset, handleSubmit } = useForm<DocumentsCompressionConfiguration>({
        defaultValues: async () =>
            (await asyncGetConfig.execute()) ?? {
                Collections: [],
                CompressAllCollections: false,
                CompressRevisions: false,
            },
    });

    const { Collections, CompressAllCollections } = useWatch({ control });

    const hasDocumentsCompression = useAppSelector(licenseSelectors.statusValue("HasDocumentsCompression"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasDocumentsCompression,
            },
        ],
    });

    useDirtyFlag(formState.isDirty);
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();
    const docsLink = useRavenLink({ hash: "WRSDA7" });

    if (asyncGetConfig.status === "not-requested" || asyncGetConfig.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetConfig.status === "error") {
        return <LoadError error="Unable to load document compression configuration" refresh={asyncGetConfig.execute} />;
    }

    const onSave: SubmitHandler<DocumentsCompressionConfiguration> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("documents-compression", "save");

            await databasesService.saveDocumentsCompression(databaseName, {
                ...formData,
                Collections: formData.CompressAllCollections ? [] : formData.Collections,
            });

            reset(formData);
        });
    };

    const infoTextSuffix = CompressAllCollections ? "all collections" : "the selected collections";

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col className="gy-sm">
                        <AboutViewHeading
                            title="Document Compression"
                            icon="documents-compression"
                            licenseBadgeText={hasDocumentsCompression ? null : "Enterprise"}
                        />
                        <Form onSubmit={handleSubmit(onSave)}>
                            <div className="hstack mb-3">
                                {hasDatabaseAdminAccess && (
                                    <>
                                        <div id="saveConfigButton" className="w-fit-content">
                                            <ButtonWithSpinner
                                                type="submit"
                                                color="primary"
                                                className="mb-3"
                                                icon="save"
                                                disabled={!formState.isDirty}
                                                isSpinning={formState.isSubmitting}
                                            >
                                                Save
                                            </ButtonWithSpinner>
                                        </div>
                                        {!hasDocumentsCompression && (
                                            <FeatureNotAvailableInYourLicensePopover target="saveConfigButton" />
                                        )}
                                    </>
                                )}
                                <FlexGrow />
                                <a href={appUrl.forStatusStorageReport(databaseName)}>
                                    <Icon icon="link" /> Storage Report
                                </a>
                            </div>

                            <Card className={classNames("p-4", { "item-disabled pe-none": !hasDocumentsCompression })}>
                                <FormCollectionsSelect
                                    control={control}
                                    collectionsFormName="Collections"
                                    collections={Collections}
                                    isAllCollectionsFormName="CompressAllCollections"
                                    isAllCollections={CompressAllCollections}
                                    allCollectionNames={allCollectionNames}
                                    setValue={setValue}
                                    customOptions={customCollectionOptions}
                                    isReadOnly={!hasDatabaseAdminAccess}
                                />
                                <Collapse isOpen={CompressAllCollections || Collections.length > 0}>
                                    <Alert color="info" className="hstack gap-3 p-3 mt-4">
                                        <Icon icon="documents-compression" className="fs-1" />
                                        <div>
                                            Documents that will be compressed:
                                            <ul className="m-0">
                                                <li>New documents created in {infoTextSuffix}</li>
                                                <li>
                                                    Existing documents that are modified & saved in {infoTextSuffix}
                                                </li>
                                            </ul>
                                        </div>
                                    </Alert>
                                </Collapse>
                            </Card>
                            <Card
                                className={classNames("p-4 mt-3", {
                                    "item-disabled pe-none": !hasDocumentsCompression,
                                })}
                            >
                                <FormSwitch
                                    control={control}
                                    name="CompressRevisions"
                                    disabled={!hasDatabaseAdminAccess}
                                >
                                    Compress revisions for all collections
                                </FormSwitch>
                            </Card>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored defaultOpen={hasDocumentsCompression ? null : "licensing"}>
                            <AccordionItemWrapper
                                targetId="aboutView"
                                icon="about"
                                color="info"
                                heading="About this view"
                                description="Get additional info on what this feature can offer you"
                            >
                                <ul>
                                    <li>
                                        Enable documents compression to achieve efficient data storage.
                                        <br />
                                        Storage space will be reduced using the zstd compression algorithm.
                                    </li>
                                    <li>
                                        Documents compression can be set for all collections, selected collections, and
                                        all revisions.
                                    </li>
                                </ul>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={docsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Compression
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={hasDocumentsCompression}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Document Compression",
        featureIcon: "documents-compression",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
