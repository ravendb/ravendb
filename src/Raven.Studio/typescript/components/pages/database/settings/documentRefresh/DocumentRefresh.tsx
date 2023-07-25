import React, { useEffect } from "react";
import { Card, CardBody, Col, Form, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { FormInput, FormSwitch } from "components/common/Form";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { tryHandleSubmit } from "components/utils/common";
import { DocumentRefreshFormData, documentRefreshYupResolver } from "./DocumentRefreshValidation";
import Code from "components/common/Code";
import { todo } from "common/developmentHelper";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { NonShardedViewProps } from "components/models/common";
import ServerRefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;
import messagePublisher = require("common/messagePublisher");
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { useAccessManager } from "components/hooks/useAccessManager";

export default function DocumentRefresh({ db }: NonShardedViewProps) {
    const { databasesService } = useServices();

    const asyncGetRefreshConfiguration = useAsyncCallback<DocumentRefreshFormData>(async () =>
        mapToFormData(await databasesService.getRefreshConfiguration(db))
    );
    const { handleSubmit, control, formState, reset, setValue } = useForm<DocumentRefreshFormData>({
        resolver: documentRefreshYupResolver,
        mode: "all",
        defaultValues: asyncGetRefreshConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control: control });
    const { reportEvent } = useEventsCollector();
    const { isAdminAccessOrAbove } = useAccessManager();

    useEffect(() => {
        if (!formValues.isRefreshFrequencyEnabled && formValues.refreshFrequency !== null) {
            setValue("refreshFrequency", null, { shouldValidate: true });
        }
        if (!formValues.isDocumentRefreshEnabled && formValues.isRefreshFrequencyEnabled) {
            setValue("isRefreshFrequencyEnabled", false, { shouldValidate: true });
        }
    }, [
        formValues.isDocumentRefreshEnabled,
        formValues.isRefreshFrequencyEnabled,
        formValues.refreshFrequency,
        setValue,
    ]);

    const onSave: SubmitHandler<DocumentRefreshFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("refresh-configuration", "save");

            await databasesService.saveRefreshConfiguration(db, {
                Disabled: !formData.isDocumentRefreshEnabled,
                RefreshFrequencyInSec: formData.isRefreshFrequencyEnabled ? formData.refreshFrequency : null,
            });

            messagePublisher.reportSuccess("Refresh configuration saved successfully");
            db.hasRefreshConfiguration(formData.isDocumentRefreshEnabled);

            reset(formData);
        });
    };

    if (asyncGetRefreshConfiguration.status === "not-requested" || asyncGetRefreshConfiguration.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetRefreshConfiguration.status === "error") {
        return <LoadError error="Unable to load document refresh" refresh={asyncGetRefreshConfiguration.execute} />;
    }

    todo("Feature", "Damian", "Render you do not have permission to this view");

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                            <AboutViewHeading title="Document Refresh" icon="expos-refresh" />
                            <ButtonWithSpinner
                                type="submit"
                                color="primary"
                                className="mb-3"
                                icon="save"
                                disabled={!formState.isDirty || !isAdminAccessOrAbove}
                                isSpinning={formState.isSubmitting}
                            >
                                Save
                            </ButtonWithSpinner>
                            <Col>
                                <Card>
                                    <CardBody>
                                        <div className="vstack gap-2">
                                            <FormSwitch
                                                name="isDocumentRefreshEnabled"
                                                control={control}
                                                disabled={formState.isSubmitting}
                                            >
                                                Enable Document Refresh
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    name="isRefreshFrequencyEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isDocumentRefreshEnabled
                                                    }
                                                >
                                                    Set custom refresh frequency
                                                </FormSwitch>
                                                <FormInput
                                                    name="refreshFrequency"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isRefreshFrequencyEnabled
                                                    }
                                                    placeholder="Default (60)"
                                                    addonText="seconds"
                                                ></FormInput>
                                            </div>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="1"
                                icon="about"
                                color="info"
                                description="Get additional info on what this feature can offer you"
                                heading="About this view"
                            >
                                <p>
                                    When <strong>Document Refresh</strong> is enabled:
                                </p>
                                <ul>
                                    <li>
                                        The server scans the database at the specified <strong>frequency</strong>,
                                        searching for documents that should be refreshed.
                                    </li>
                                    <li>
                                        Any document that has a <code>@refresh</code> metadata property whose time has
                                        passed at the time of the scan will be modified by removing this property.
                                    </li>
                                    <li>
                                        This modification will trigger any processes related to the document, such as:
                                        re-indexing or taking part in an ongoing-task (e.g. Replication, ETL,
                                        Subscriptions, etc.), as defined by your configuration.
                                    </li>
                                </ul>
                                <p>Sample document:</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/1PKUYJ/6.0/Csharp" target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Refresh
                                </a>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

function mapToFormData(dto: ServerRefreshConfiguration): DocumentRefreshFormData {
    if (!dto) {
        return {
            isDocumentRefreshEnabled: false,
            isRefreshFrequencyEnabled: false,
            refreshFrequency: null,
        };
    }

    return {
        isDocumentRefreshEnabled: !dto.Disabled,
        isRefreshFrequencyEnabled: dto.RefreshFrequencyInSec != null,
        refreshFrequency: dto.RefreshFrequencyInSec,
    };
}

const codeExample = `{
    "Example": "Set a timestamp in the @refresh metadata property",
    "@metadata": {
        "@collection": "Foo",
        "@refresh": "2023-07-16T08:00:00.0000000Z"
    }
}`;
