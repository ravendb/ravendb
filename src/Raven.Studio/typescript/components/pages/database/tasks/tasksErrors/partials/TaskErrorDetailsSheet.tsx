import React, { ReactNode } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import {
    SheetNavigationButtons,
    SheetSlideTransition,
    useSheetSlideNavigation,
} from "components/common/splitView/SheetSlideNavigation";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import genUtils from "common/generalUtils";
import moment from "moment";
import { FlatError, getStepIcon, getTaskTypeDisplay, healthStatusToBadge } from "../utils/tasksErrorsUtils";
import Code from "components/common/Code";

interface SheetDetailRowProps {
    children: ReactNode;
    className?: string;
}

function SheetDetailRow({ children, className }: SheetDetailRowProps) {
    return (
        <div
            className={classNames(
                "d-flex justify-content-between align-items-center pb-2 pt-2 border-bottom border-secondary",
                className
            )}
        >
            {children}
        </div>
    );
}

interface TaskErrorDetailsSheetProps {
    error: FlatError;
    allErrors?: FlatError[];
    initialIndex?: number;
}

export default function TaskErrorDetailsSheet({
    error: initialError,
    allErrors = [],
    initialIndex = 0,
}: TaskErrorDetailsSheetProps) {
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { currentIndex, direction, navigate, hasPrevious, hasNext } = useSheetSlideNavigation(
        initialIndex,
        allErrors.length
    );
    const error = allErrors.length > 0 ? allErrors[currentIndex] : initialError;

    const { bg, icon, label } = healthStatusToBadge(error.healthStatus);
    const stepIcon = getStepIcon(error.Step);
    const { icon: taskTypeIcon, label: taskTypeLabel } = getTaskTypeDisplay(error.category, error.etlType);

    return (
        <ViewSheet>
            <ViewSheet.Header>
                <h3 className="mb-0">
                    <Icon icon="warning" color="warning" />
                    Task error details
                </h3>
            </ViewSheet.Header>
            <ViewSheet.Body className="m-2">
                <SheetSlideTransition currentIndex={currentIndex} direction={direction} className="vstack gap-0">
                    {error.etlName && error.transformationName ? (
                        <SheetDetailRow>
                            <div className="small">Task name/Script name</div>
                            <div className="d-flex align-items-center text-right">
                                {taskTypeIcon && <Icon icon={taskTypeIcon} />}
                                <div>
                                    {error.etlName}/{error.transformationName}
                                </div>
                            </div>
                        </SheetDetailRow>
                    ) : (
                        error.TaskName && (
                            <SheetDetailRow>
                                <div className="small">Task name/Script name</div>
                                <div className="d-flex align-items-center">
                                    {taskTypeIcon && <Icon icon={taskTypeIcon} />}
                                    <div>{error.TaskName}</div>
                                </div>
                            </SheetDetailRow>
                        )
                    )}

                    <SheetDetailRow>
                        <div className="small">Task type</div>
                        <div className="d-flex align-items-center">
                            {taskTypeIcon && <Icon icon={taskTypeIcon} />}
                            {taskTypeLabel}
                        </div>
                    </SheetDetailRow>

                    {error.errorType && (
                        <SheetDetailRow>
                            <div className="small">Error type</div>
                            <Badge
                                bg={error.errorType === "Item" ? "secondary" : "info"}
                                className="rounded-pill cell-value"
                            >
                                <Icon icon={error.errorType === "Item" ? "tasks" : "hammer-driver"} />
                                {error.errorType === "Item" ? "Item Error" : "Process Error"}
                            </Badge>
                        </SheetDetailRow>
                    )}

                    {error.Step && (
                        <SheetDetailRow>
                            <div className="small">Error step</div>
                            <div>
                                {stepIcon && <Icon icon={stepIcon} />}
                                {error.Step}
                            </div>
                        </SheetDetailRow>
                    )}

                    {error.errorType === "Item" && error.DocumentId && (
                        <SheetDetailRow>
                            <div className="small">Document ID</div>
                            <CellDocumentValue value={error.DocumentId} databaseName={dbName} hasHyperlinkForIds />
                        </SheetDetailRow>
                    )}

                    {error.errorType === "Process" && error.AffectedDocumentsCount != null && (
                        <SheetDetailRow>
                            <div className="small">Affected Documents</div>
                            <div>{error.AffectedDocumentsCount}</div>
                        </SheetDetailRow>
                    )}

                    {error.CreatedAt && (
                        <SheetDetailRow>
                            <div className="small">Date</div>
                            <div className="vstack align-items-end">
                                <span>{moment(error.CreatedAt).format(genUtils.dateFormat)}</span>
                                <small className="text-muted">{moment(error.CreatedAt).fromNow()}</small>
                            </div>
                        </SheetDetailRow>
                    )}

                    {error.healthStatus && (
                        <SheetDetailRow>
                            <div className="small">Current task health</div>
                            <Badge bg={bg} className="rounded-pill">
                                <Icon icon={icon} />
                                {label}
                            </Badge>
                        </SheetDetailRow>
                    )}

                    <SheetDetailRow className="border-bottom-0">
                        <div className="small">Localization</div>
                        <div className="d-flex align-items-center gap-2">
                            <div className="d-flex align-items-center justify-content-center">
                                <Icon icon="node" color="node" />
                                {error.nodeTag}
                            </div>
                            {error.shardNumber != null && (
                                <div className="d-flex align-items-center justify-content-center">
                                    <Icon icon="shard" color="shard" />#{error.shardNumber}
                                </div>
                            )}
                        </div>
                    </SheetDetailRow>

                    {error.Error && (
                        <div>
                            <Code code={error.Error} language="csharp" />
                        </div>
                    )}
                </SheetSlideTransition>
            </ViewSheet.Body>
            <ViewSheet.Footer className="d-flex justify-content-between">
                <SheetNavigationButtons hasPrevious={hasPrevious} hasNext={hasNext} navigate={navigate} />
            </ViewSheet.Footer>
        </ViewSheet>
    );
}
