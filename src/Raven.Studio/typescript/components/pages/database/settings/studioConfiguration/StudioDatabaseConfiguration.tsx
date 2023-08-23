import React from "react";
import {
    Button,
    Card,
    CardBody,
    Col,
    Form,
    InputGroup,
    Label,
    PopoverBody,
    Row,
    UncontrolledPopover,
} from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormSelect, FormSwitch } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    StudioDatabaseConfigurationFormData,
    studioDatabaseConfigurationYupResolver,
} from "./StudioDatabaseConfigurationValidation";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { studioEnvironmentOptions } from "components/common/studioConfiguration/StudioConfigurationUtils";
import { useServices } from "components/hooks/useServices";
import appUrl from "common/appUrl";
import { NonShardedViewProps } from "components/models/common";
import {
    AboutViewAnchored,
    AboutViewHeading,
    AccordionItemLicensing,
    AccordionItemWrapper,
} from "components/common/AboutView";

interface StudioDatabaseConfigurationProps extends NonShardedViewProps {
    licenseType?: string;
}

export default function StudioDatabaseConfiguration({ db, licenseType }: StudioDatabaseConfigurationProps) {
    const { databasesService } = useServices();

    const asyncDatabaseSettings = useAsyncCallback<StudioDatabaseConfigurationFormData>(async () => {
        const settings = await databasesService.getDatabaseStudioConfiguration(db);

        return {
            Environment: settings ? settings.Environment : "None",
            DisableAutoIndexCreation: settings ? settings.DisableAutoIndexCreation : false,
            Disabled: settings ? settings.Disabled : false,
        };
    });

    const { handleSubmit, control, formState, reset } = useForm<StudioDatabaseConfigurationFormData>({
        resolver: studioDatabaseConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncDatabaseSettings.execute,
    });

    useDirtyFlag(formState.isDirty);

    const { reportEvent } = useEventsCollector();

    const onSave: SubmitHandler<StudioDatabaseConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("studio-configuration-database", "save");
            databasesService.saveDatabaseStudioConfiguration(formData, db);
            reset(formData);
        });
    };

    const onRefresh = async () => {
        reset(await asyncDatabaseSettings.execute());
    };

    if (asyncDatabaseSettings.status === "not-requested" || asyncDatabaseSettings.status === "loading") {
        return <LoadingView />;
    }

    if (asyncDatabaseSettings.status === "error") {
        return <LoadError error="Unable to load studio configuration" refresh={onRefresh} />;
    }

    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading
                        icon="database-studio-configuration"
                        title="Studio Configuration"
                        badge={licenseType === "community"}
                        badgeText={licenseType === "community" ? "Professional +" : undefined}
                    />
                    <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                        <div className="d-flex align-items-center justify-content-between">
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
                            <small title="Navigate to the server-wide Client Configuration View">
                                <a target="_blank" href={appUrl.forGlobalClientConfiguration()}>
                                    <Icon icon="link" />
                                    Go to Server-Wide Studio Configuration View
                                </a>
                            </small>
                        </div>
                        <div className={licenseType === "community" ? "item-disabled pe-none" : ""}>
                            <Card id="popoverContainer">
                                <CardBody className="d-flex flex-center flex-column flex-wrap gap-4">
                                    <InputGroup className="gap-1 flex-wrap flex-column">
                                        <Label className="mb-0 md-label">
                                            Environment <Icon icon="info" color="info" id="environmentInfo" />
                                            <UncontrolledPopover
                                                target="environmentInfo"
                                                placement="right"
                                                trigger="hover"
                                                container="popoverContainer"
                                            >
                                                <PopoverBody>
                                                    Change the studio environment tag. This does not affect settings or
                                                    features.
                                                </PopoverBody>
                                            </UncontrolledPopover>
                                        </Label>
                                        <FormSelect
                                            control={control}
                                            name="Environment"
                                            options={studioEnvironmentOptions}
                                        ></FormSelect>
                                    </InputGroup>
                                </CardBody>
                            </Card>
                            <Card className="mt-3" id="disableAutoIndexesContainer">
                                <CardBody>
                                    <div className="d-flex flex-column">
                                        <UncontrolledPopover
                                            target="disableAutoIndexesInfo"
                                            placement="right"
                                            trigger="hover"
                                            container="disableAutoIndexesContainer"
                                        >
                                            <PopoverBody>
                                                <ul className="mb-0">
                                                    <li>
                                                        Toggle on to disable creating new Auto-Indexes when making a
                                                        <strong> dynamic query</strong>.
                                                    </li>
                                                    <li>
                                                        Query results will be returned only when a matching Auto-Index
                                                        already exists.
                                                    </li>
                                                </ul>
                                            </PopoverBody>
                                        </UncontrolledPopover>
                                        <FormSwitch control={control} name="DisableAutoIndexCreation">
                                            Disable creating new Auto-Indexes{" "}
                                            <Icon icon="info" color="info" id="disableAutoIndexesInfo" />
                                        </FormSwitch>
                                    </div>
                                </CardBody>
                            </Card>
                        </div>
                    </Form>
                </Col>
                <Col sm={12} md={4}>
                    <AboutViewAnchored>
                        <AccordionItemWrapper
                            icon="about"
                            color="info"
                            heading="About this view"
                            description="Get additional info on what this feature can offer you"
                            targetId="1"
                        >
                            <p>
                                <strong>Studio Configuration</strong> lorem ipsum
                            </p>
                            <hr />
                            <div className="small-label mb-2">useful links</div>
                            <a href="https://ravendb.net/l/HIR1VP/6.0/Csharp" target="_blank">
                                <Icon icon="newtab" /> Docs - Studio Configuration
                            </a>
                        </AccordionItemWrapper>
                        {licenseType === "community" && (
                            <AccordionItemWrapper
                                icon="license"
                                color="warning"
                                heading="Licensing"
                                description="See which plans offer this and more exciting features"
                                targetId="licensing"
                                pill
                                pillText="Upgrade available"
                                pillIcon="star-filled"
                            >
                                <AccordionItemLicensing
                                    description="This feature is not available in your license. Unleash the full potential and upgrade your plan."
                                    featureName="Studio Configuration"
                                    featureIcon="database-studio-configuration"
                                    checkedLicenses={["Professional", "Enterprise"]}
                                >
                                    <p className="lead fs-4">Get your license expanded</p>
                                    <div className="mb-3">
                                        <Button color="primary" className="rounded-pill">
                                            <Icon icon="notifications" />
                                            Contact us
                                        </Button>
                                    </div>
                                    <small>
                                        <a href="https://ravendb.net/buy" target="_blank" className="text-muted">
                                            See pricing plans
                                        </a>
                                    </small>
                                </AccordionItemLicensing>
                            </AccordionItemWrapper>
                        )}
                    </AboutViewAnchored>
                </Col>
            </Row>
        </div>
    );
}
