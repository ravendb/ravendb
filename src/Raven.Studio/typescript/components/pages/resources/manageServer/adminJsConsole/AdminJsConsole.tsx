import React from "react";
import { Card, Alert, CardHeader, CardBody, Row, Col, Form } from "reactstrap";
import { Icon } from "components/common/Icon";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import AceEditor from "components/common/AceEditor";
import { todo } from "common/developmentHelper";
import { SubmitHandler, useForm } from "react-hook-form";
import { tryHandleSubmit } from "components/utils/common";
import { AdminJsConsoleFormData, adminJsConsoleYupResolver } from "./AdminJsConsoleValidation";
import { useServices } from "components/hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAsyncCallback } from "react-async-hook";
import { FormAceEditor, FormSelect } from "components/common/Form";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import {
    OptionWithIconAndSeparator,
    SelectOptionWithIconAndSeparator,
    SingleValueWithIcon,
} from "components/common/select/Select";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import "./AdminJsConsole.scss";
import RunScriptButton from "components/common/RunScriptButton";
import useBoolean from "components/hooks/useBoolean";
import { useRavenLink } from "components/hooks/useRavenLink";

const serverTargetValue = "Server";

export default function AdminJSConsole() {
    todo("Feature", "Damian", "issue: RavenDB-7588");

    const { value: isScriptValid, setValue: setIsScriptValid } = useBoolean(true);
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();
    const asyncRunAdminJsScript = useAsyncCallback(manageServerService.runAdminJsScript);
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);

    const allDatabaseNames = allDatabases.flatMap((db) => (db.isSharded ? db.shards.map((x) => x.name) : [db.name]));

    const adminJsConsoleDocsLink = useRavenLink({ hash: "IBUJ7M" });

    const allTargets: SelectOptionWithIconAndSeparator[] = [
        {
            value: serverTargetValue,
            label: "Server",
            icon: "server",
            horizontalSeparatorLine: allDatabaseNames.length > 0,
        },
        ...allDatabaseNames.map(
            (x) => ({ value: x, label: x, icon: "database" }) satisfies SelectOptionWithIconAndSeparator
        ),
    ];

    const { handleSubmit, control, reset, formState, watch } = useForm<AdminJsConsoleFormData>({
        resolver: adminJsConsoleYupResolver,
        mode: "all",
        defaultValues: {
            target: allTargets[0].value,
            scriptText: "",
        },
    });

    useDirtyFlag(formState.isDirty);

    const onSave: SubmitHandler<AdminJsConsoleFormData> = async (formData) => {
        reportEvent("console", "execute");

        tryHandleSubmit(async () => {
            const databaseTarget = formData.target !== serverTargetValue ? formData.target : undefined;
            await asyncRunAdminJsScript.execute(formData.scriptText, databaseTarget);

            reset(formData);
        });
    };

    const accessibleVariable = watch("target") === serverTargetValue ? "server" : "database";

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Admin JS Console" icon="administrator-js-console" />
                        <Col>
                            <Alert color="warning hstack gap-4 mb-3">
                                <div className="flex-shrink-0">
                                    <Icon icon="warning" /> WARNING
                                </div>
                                <div>
                                    Do not use the console unless you are sure about what you&apos;re doing. Running a
                                    script in the Admin Console could cause your server to crash, cause loss of data, or
                                    other irreversible harm.
                                </div>
                            </Alert>

                            <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                                <Card>
                                    <CardHeader className="hstack gap-4 flex-wrap">
                                        <h3 className="m-0">Script target</h3>
                                        <FormSelect
                                            control={control}
                                            name="target"
                                            options={allTargets}
                                            maxMenuHeight={200}
                                            components={{
                                                Option: OptionWithIconAndSeparator,
                                                SingleValue: SingleValueWithIcon,
                                            }}
                                        />
                                        <div className="text-info">
                                            Accessible within the script under <code>{accessibleVariable}</code>{" "}
                                            variable
                                        </div>
                                    </CardHeader>
                                    <CardBody>
                                        <div className="admin-js-console-grid">
                                            <div>
                                                <h3>Script</h3>
                                            </div>
                                            <FormAceEditor
                                                control={control}
                                                name="scriptText"
                                                execute={handleSubmit(onSave)}
                                                mode="javascript"
                                                height="200px"
                                                setIsValid={(x) => setIsScriptValid(x)}
                                            />
                                            <RunScriptButton
                                                type="submit"
                                                isSpinning={asyncRunAdminJsScript.status === "loading"}
                                                disabled={!isScriptValid}
                                            />
                                            <div>
                                                <h3>Script result</h3>
                                            </div>
                                            <AceEditor
                                                mode="javascript"
                                                readOnly
                                                value={JSON.stringify(asyncRunAdminJsScript.result?.Result, null, 4)}
                                                height="300px"
                                            />
                                        </div>
                                    </CardBody>
                                </Card>
                            </Form>
                        </Col>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="1"
                                icon="about"
                                color="info"
                                description="Get additional info on this feature"
                                heading="About this view"
                            >
                                <p>
                                    <strong>Admin JS Console</strong> is a specialized feature primarily intended for
                                    resolving server errors. It provides a direct interface to the underlying system,
                                    granting the capacity to execute scripts for intricate server operations.
                                </p>
                                <p>
                                    It is predominantly intended for advanced troubleshooting and rectification
                                    procedures executed by system administrators or RavenDB support.
                                </p>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={adminJsConsoleDocsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Admin JS Console
                                </a>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
