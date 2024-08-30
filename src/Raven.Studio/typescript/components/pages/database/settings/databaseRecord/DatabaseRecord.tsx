import React, { useCallback, useEffect, useRef, useState } from "react";
import { Alert, Button, Card, CardBody, Col, InputGroup, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import useConfirm from "components/common/ConfirmDialog";
import classNames from "classnames";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { Switch } from "components/common/Checkbox";
import AceEditor from "components/common/AceEditor";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import genUtils from "common/generalUtils";
import DatabaseRecordAboutView from "./DatabaseRecordAboutView";
import ReactAce from "react-ace/lib/ace";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface VisibleDocument {
    text: string;
    isFromServer: boolean;
}

export default function DatabaseRecord() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);

    const { databasesService } = useServices();
    const confirm = useConfirm();
    const { reportEvent } = useEventsCollector();
    const aceRef = useRef<ReactAce>(null);

    const { value: isEditMode, toggle: toggleIsEditMode } = useBoolean(false);
    const { value: isHideEmptyValues, toggle: toggleIsHideEmptyValues } = useBoolean(false);
    const { value: isCollapsed, setValue: setIsCollapsed } = useBoolean(false);
    const [visibleDocument, setVisibleDocument] = useState<VisibleDocument>(null);

    const asyncGetDatabaseRecord = useAsyncCallback(
        async (reportRefreshProgress: boolean) =>
            databasesService.getDatabaseRecord(databaseName, reportRefreshProgress),
        {
            onSuccess: () => {
                setIsCollapsed(false);
            },
        }
    );

    const asyncSaveDatabaseRecord = useAsyncCallback(async () => {
        const dto: documentDto = JSON.parse(visibleDocument.text);
        dto.Settings = genUtils.flattenObj(dto.Settings, "");

        await databasesService.saveDatabaseRecord(databaseName, dto, dto.Etag);
        await asyncGetDatabaseRecord.execute(false);
    });

    const collapse = useCallback(() => {
        if (aceRef.current) {
            aceRef.current.editor.session.foldAll(1);
            setIsCollapsed(true);
        }
    }, [setIsCollapsed]);

    const expand = () => {
        if (aceRef.current) {
            aceRef.current.editor.session.unfold(null);
            setIsCollapsed(false);
        }
    };

    const updateVisibleDocumentText = useCallback(() => {
        const docText = genUtils.stringify(asyncGetDatabaseRecord.result.toDto(), isHideEmptyValues);

        setVisibleDocument({ text: docText, isFromServer: true });
    }, [asyncGetDatabaseRecord.result, isHideEmptyValues]);

    useEffect(() => {
        const getData = async () => {
            if (asyncGetDatabaseRecord.status === "not-requested" && isOperatorOrAbove) {
                try {
                    await asyncGetDatabaseRecord.execute(false);
                } catch (error) {
                    messagePublisher.reportError("Error fetching database record!", error.message, error.name);
                }
            }
        };
        getData();
    }, [asyncGetDatabaseRecord, isOperatorOrAbove]);

    useEffect(() => {
        if (asyncGetDatabaseRecord.result) {
            updateVisibleDocumentText();
        }
    }, [asyncGetDatabaseRecord.result, isHideEmptyValues, updateVisibleDocumentText]);

    useEffect(() => {
        if (visibleDocument?.isFromServer) {
            collapse();
        }
    }, [collapse, visibleDocument]);

    const toggleEditMode = async () => {
        const confirmed = await confirm({
            icon: "database-record",
            actionColor: "warning",
            title: "You're about to enter edit mode",
            message: <EditModeRiskAlert />,
            confirmText: "I understand the risk and want to proceed",
        });

        if (confirmed) {
            toggleIsEditMode();
        }
    };

    const saveDatabaseRecord = async () => {
        if (aceRef.current.editor?.session?.getAnnotations()?.some((x) => x.type === "error")) {
            return;
        }

        const confirmed = await confirm({
            icon: "save",
            actionColor: "warning",
            title: "Do you want to save changes?",
            confirmText: "Yes, save changes",
        });

        if (confirmed) {
            await asyncSaveDatabaseRecord.execute();
            toggleIsEditMode();
        }
    };

    const discardDatabaseRecord = async () => {
        const confirmed = await confirm({
            icon: "database-record",
            actionColor: "primary",
            title: "Do you want to discard changes?",
            confirmText: "Yes, discard changes",
        });

        if (confirmed) {
            updateVisibleDocumentText();
            toggleIsEditMode();
        }
    };

    const refresh = () => {
        reportEvent("database-record", "refresh");
        asyncGetDatabaseRecord.execute(true);
    };

    if (asyncGetDatabaseRecord.status === "error") {
        return (
            <LoadError error="Unable to load database record" refresh={() => asyncGetDatabaseRecord.execute(true)} />
        );
    }

    return (
        <div className="content-margin">
            {isOperatorOrAbove ? (
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Database Record" icon="database-record" />
                        <div
                            className={classNames("d-flex gap-3 flex-wrap mb-3", {
                                "justify-content-between": !isEditMode,
                            })}
                        >
                            {isEditMode ? (
                                <>
                                    <ButtonWithSpinner
                                        color="primary"
                                        icon="save"
                                        onClick={saveDatabaseRecord}
                                        isSpinning={asyncSaveDatabaseRecord.loading}
                                    >
                                        Save
                                    </ButtonWithSpinner>
                                    <Button color="secondary" onClick={discardDatabaseRecord}>
                                        Cancel
                                    </Button>
                                </>
                            ) : (
                                <>
                                    <ButtonWithSpinner
                                        color="primary"
                                        onClick={refresh}
                                        isSpinning={asyncGetDatabaseRecord.loading}
                                        icon="refresh"
                                    >
                                        Refresh
                                    </ButtonWithSpinner>
                                    <Button
                                        color="secondary"
                                        onClick={toggleEditMode}
                                        disabled={asyncGetDatabaseRecord.loading}
                                    >
                                        <Icon icon="edit" />
                                        Edit record
                                    </Button>
                                </>
                            )}
                        </div>

                        <Card>
                            <CardBody className="d-flex flex-center flex-column flex-wrap gap-4">
                                <InputGroup className="gap-1 flex-wrap flex-column">
                                    <div
                                        className={classNames(
                                            "d-flex flex-wrap",
                                            isEditMode ? "justify-content-end" : "justify-content-between"
                                        )}
                                    >
                                        {!isEditMode && (
                                            <Switch
                                                type="switch"
                                                color="primary"
                                                selected={isHideEmptyValues}
                                                toggleSelection={toggleIsHideEmptyValues}
                                            >
                                                Hide empty values
                                            </Switch>
                                        )}
                                        <Button
                                            color="link"
                                            size="xs"
                                            className="p-0"
                                            onClick={isCollapsed ? expand : collapse}
                                            disabled={asyncGetDatabaseRecord.loading}
                                        >
                                            {isCollapsed ? (
                                                <>
                                                    <Icon icon="expand-vertical" /> Expand record
                                                </>
                                            ) : (
                                                <>
                                                    <Icon icon="collapse-vertical" /> Collapse record
                                                </>
                                            )}
                                        </Button>
                                    </div>
                                    <AceEditor
                                        aceRef={aceRef}
                                        mode="json"
                                        height="600px"
                                        value={visibleDocument?.text}
                                        onChange={(x) => setVisibleDocument({ text: x, isFromServer: false })}
                                        readOnly={!isEditMode}
                                    />
                                </InputGroup>
                            </CardBody>
                        </Card>
                    </Col>
                    <Col sm={12} lg={4}>
                        <DatabaseRecordAboutView />
                    </Col>
                </Row>
            ) : (
                <FeatureNotAvailable badgeText="Insufficient access">
                    You are not authorized to view this page
                </FeatureNotAvailable>
            )}
        </div>
    );
}

function EditModeRiskAlert() {
    return (
        <Alert color="warning">
            <Icon icon="warning" />
            Tampering with the Database Record may result in unwanted behavior including loss of the database along with
            all its data.
        </Alert>
    );
}
