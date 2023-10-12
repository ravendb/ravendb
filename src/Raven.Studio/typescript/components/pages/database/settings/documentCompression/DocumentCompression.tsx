import React, { useState } from "react";
import { Col, Button, Row, Card, Collapse, Form } from "reactstrap";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import { EmptySet } from "components/common/EmptySet";
import { FlexGrow } from "components/common/FlexGrow";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useEnterpriseLicenseAvailability } from "components/utils/licenseLimitsUtils";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormRadioToggleWithIcon, FormSelectCreatable, FormSwitch } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { NonShardedViewProps } from "components/models/common";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import { useRavenLink } from "components/hooks/useRavenLink";
import { DevTool } from "@hookform/devtools";
import classNames from "classnames";
import { tryHandleSubmit } from "components/utils/common";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import FeatureNotAvailableInYourLicensePopover from "components/common/FeatureNotAvailableInYourLicensePopover";
import DocumentsCompressionConfiguration = Raven.Client.ServerWide.DocumentsCompressionConfiguration;
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { SelectOption } from "components/common/select/Select";
import { useAppUrls } from "components/hooks/useAppUrls";

todo("Styling", "ANY", "Collection list item hover");
todo("Styling", "ANY", "Remove collection button");
todo("Styling", "ANY", "RadioToggleWithIcon when disabled");

export default function DocumentCompression({ db }: NonShardedViewProps) {
    const { databasesService } = useServices();
    const asyncGetConfig = useAsyncCallback(() => databasesService.getDocumentsCompressionConfiguration(db));

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );

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
    const featureAvailability = useEnterpriseLicenseAvailability(hasDocumentsCompression);
    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    useDirtyFlag(formState.isDirty);
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();
    const docsLink = useRavenLink({ hash: "WRSDA7" });

    const [collectionOptions, setCollectionOptions] = useState<SelectOption[]>(
        allCollectionNames.map((x) => ({ label: x, value: x }))
    );

    if (asyncGetConfig.status === "not-requested" || asyncGetConfig.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetConfig.status === "error") {
        return <LoadError error="Unable to load document compression configuration" refresh={asyncGetConfig.execute} />;
    }

    const onCreateOption = (name: string) => {
        const newOption: SelectOption = { value: name, label: name };

        setCollectionOptions((options) => [...options, newOption]);
        setValue("Collections", [...Collections, name], { shouldDirty: true });
    };

    const onRemoveCollection = (name: string) => {
        setValue(
            "Collections",
            Collections.filter((x) => x !== name),
            { shouldDirty: true }
        );
    };

    const onSave: SubmitHandler<DocumentsCompressionConfiguration> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("documents-compression", "save");
            await databasesService.saveDocumentsCompression(db, formData);
            reset(formData);
        });
    };

    const infoTextSuffix = CompressAllCollections ? "all collections" : "the selected collections";

    return (
        <div className="content-margin">
            <DevTool control={control} />
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col className="gy-sm">
                        <AboutViewHeading
                            title="Document Compression"
                            icon="documents-compression"
                            licenseBadgeText={hasDocumentsCompression ? null : "Enterprise"}
                        />
                        <Form
                            onSubmit={handleSubmit(onSave)}
                            className={classNames({ "item-disabled pe-none": !hasDocumentsCompression })}
                        >
                            <div className="hstack mb-3">
                                {isDatabaseAdmin && (
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
                                <a href={appUrl.forStatusStorageReport(db)}>
                                    <Icon icon="link" /> Storage Report
                                </a>
                            </div>

                            <Card className="p-4">
                                <FormRadioToggleWithIcon
                                    control={control}
                                    name="CompressAllCollections"
                                    leftItem={leftRadioToggleItem}
                                    rightItem={rightRadioToggleItem}
                                    className="mb-4 d-flex justify-content-center"
                                    disabled={!isDatabaseAdmin}
                                />
                                <Collapse isOpen={!CompressAllCollections} className="pb-2">
                                    {isDatabaseAdmin && (
                                        <Row>
                                            <Col>
                                                <FormSelectCreatable
                                                    control={control}
                                                    name="Collections"
                                                    options={collectionOptions}
                                                    onCreateOption={onCreateOption}
                                                    isMulti
                                                    controlShouldRenderValue={false}
                                                    isClearable={false}
                                                    placeholder="Select collection (or enter new collection)"
                                                />
                                            </Col>
                                            <Col sm="auto" className="d-flex">
                                                <Button
                                                    color="info"
                                                    onClick={() => setValue("Collections", allCollectionNames)}
                                                >
                                                    <Icon icon="documents" addon="plus" /> Add All
                                                </Button>
                                            </Col>
                                        </Row>
                                    )}
                                    <h3 className="mt-3">Selected Collections:</h3>
                                    <div className="well p-2">
                                        <div className="simple-item-list">
                                            {Collections.map((name) => (
                                                <div key={name} className="p-1 hstack blink-style">
                                                    <div className="flex-grow-1 pl-2">{name}</div>
                                                    {isDatabaseAdmin && (
                                                        <Button
                                                            color="link"
                                                            size="xs"
                                                            onClick={() => onRemoveCollection(name)}
                                                        >
                                                            <Icon icon="trash" />
                                                        </Button>
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                        <Collapse isOpen={Collections.length === 0}>
                                            <EmptySet>No collections have been selected</EmptySet>
                                        </Collapse>
                                    </div>
                                </Collapse>
                                <Collapse isOpen={CompressAllCollections || Collections.length > 0}>
                                    <div className="bg-faded-info hstack gap-3 p-3 mt-3">
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
                                    </div>
                                </Collapse>
                            </Card>
                            <Card className="p-4 mt-3">
                                <FormSwitch control={control} name="CompressRevisions" disabled={!isDatabaseAdmin}>
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

const leftRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "Compress selected collections",
    value: false,
    iconName: "document",
};

const rightRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "Compress all collections",
    value: true,
    iconName: "documents",
};
