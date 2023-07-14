import React from "react";
import { Card, CardBody, Col, Form, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { FormInput, FormSwitch } from "components/common/Form";
import { SubmitHandler, useForm } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { tryHandleSubmit } from "components/utils/common";
import { DocumentRefreshFormData, documentRefreshYupResolver } from "./DocumentRefreshValidation";
import Code from "components/common/Code";
import { todo } from "common/developmentHelper";

export default function DocumentRefresh() {
    const asyncGlobalSettings = useAsyncCallback<DocumentRefreshFormData>(null);

    const { handleSubmit, control, formState, reset } = useForm<DocumentRefreshFormData>({
        resolver: documentRefreshYupResolver,
        mode: "all",
        defaultValues: asyncGlobalSettings.execute,
    });

    useDirtyFlag(formState.isDirty);

    const onSave: SubmitHandler<DocumentRefreshFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reset(formData);
        });
    };

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
                                disabled={!formState.isDirty}
                                isSpinning={formState.isSubmitting}
                            >
                                Save
                            </ButtonWithSpinner>
                            <Col>
                                <Card>
                                    <CardBody>
                                        <div className="vstack gap-2">
                                            <FormSwitch name="isDocumentRefreshEnabled" control={control}>
                                                Enable Document Refresh
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    name="isRefreshFrequencyEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                >
                                                    Set custom refresh frequency
                                                </FormSwitch>
                                                <FormInput
                                                    name="isRefreshFrequencyInSec"
                                                    control={control}
                                                    type="number"
                                                    placeholder="Default (60)"
                                                    addonTextEnabled
                                                    addonTextContent="seconds"
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
                                    Enabling <strong>Document Refresh</strong> will refresh documents that have a{" "}
                                    <code>@refresh</code> flag in the metadata at the time specified by the flag. At
                                    that time RavenDB will <strong>remove</strong> the <code>@refresh</code> flag
                                    causing the document to automatically update.
                                </p>
                                <p>As a result, and depending on your tasks and indexing configuration:</p>
                                <ul>
                                    <li>A document will be re-indexed</li>
                                    <li>
                                        Ongoing-tasks such as Replication, ETL, Subscriptions, etc. will be triggered
                                    </li>
                                </ul>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/1PKUYJ/6.0/Csharp" target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Refresh
                                </a>
                            </AccordionItemWrapper>
                            <AccordionItemWrapper
                                targetId="2"
                                icon="road-cone"
                                color="success"
                                description="Learn how to get the most of Document Refresh"
                                heading="Examples of use"
                            >
                                <p>
                                    <strong>To set the refresh time:</strong> enter the appropriate date in the metadata{" "}
                                    <code>@refresh</code> property.
                                </p>
                                <p>
                                    <strong>Note:</strong> RavenDB scans which documents should be refreshed at the
                                    frequency specified. The actual refresh time can increase (up to) that value.
                                </p>
                                <Code code={codeExample} language="javascript"></Code>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

const codeExample = `
{
    "Example": "This is an example of a document with @refresh flag set",
    "@metadata": {
        "@collection": "Foo",
        "@refresh": "2017-10-10T08:00:00.0000000Z"
    }
}`;
