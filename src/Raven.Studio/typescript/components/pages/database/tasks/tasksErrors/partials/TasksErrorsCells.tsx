import React, { useMemo } from "react";
import { CellContext } from "@tanstack/react-table";
import { Icon } from "components/common/Icon";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import moment from "moment";
import genUtils from "common/generalUtils";
import EtlErrorDetailsSheet from "./EtlErrorDetailsSheet";
import {
    AI_ONLY_TASK_TYPES,
    EtlErrorStep,
    EtlHealthStatus,
    FlatError,
    getEtlEditLink,
    getEtlTypeIcon,
    getEtlTypeLabel,
    getPopoverMessageForErrorType,
    getPopoverMessageForTaskHealth,
    getStepIcon,
    healthStatusToBadge,
} from "../utils/tasksErrorsUtils";
import colorsManager from "common/colorsManager";

export { CellWithCopyWrapper };

export const CellErrorStepWrapper = ({ getValue }: CellContext<FlatError, EtlErrorStep>) => {
    const value = getValue();

    if (!value) {
        return <CellValue value="-" />;
    }

    const stepIcon = getStepIcon(value);
    return (
        <div className="cell-value value-string">
            {stepIcon && <Icon icon={stepIcon} />}
            <CellValue value={value} />
        </div>
    );
};

export const CellErrorTypeWrapper = ({ getValue }: CellContext<FlatError, "Item" | "Process">) => {
    const value = getValue();
    return (
        <PopoverWithHoverWrapper
            message={getPopoverMessageForErrorType(value)}
            wrapperClassName="d-flex align-items-center h-100"
            inline={false}
        >
            <Badge bg={value === "Item" ? "secondary" : "info"} className="rounded-pill cell-value">
                <Icon icon={value === "Item" ? "tasks" : "hammer-driver"} />
                {value === "Item" ? "Item Error" : "Process Error"}
            </Badge>
        </PopoverWithHoverWrapper>
    );
};

export const CellShardValueWrapper = ({ getValue }: CellContext<FlatError, string>) => {
    return (
        <>
            <Icon icon="shard" color="shard" />
            <CellValue value={"#" + getValue()} />
        </>
    );
};

interface NodeCircleProps {
    nodeTag: string;
}

export function NodeCircle({ nodeTag }: NodeCircleProps) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const nodeColors = colorsManager.nodeColors;
    const nodeIndex = db.nodes.findIndex((n) => n.tag === nodeTag);

    return (
        <div
            className="node-circle rounded-circle p-2 d-flex text-black justify-content-center align-items-center fw-bold"
            style={{ backgroundColor: nodeColors.at(nodeIndex) }}
        >
            {nodeTag}
        </div>
    );
}

export const CellNodeValueWrapper = ({ getValue }: CellContext<FlatError, string>) => {
    return <NodeCircle nodeTag={getValue()} />;
};

export const CellValueButtonWrapper = (args: CellContext<FlatError, unknown>) => {
    const { open } = useViewSheet();

    const handleOpenSheet = () => {
        const allRows = args.table.getRowModel().rows;
        const allErrors = allRows.map((r) => r.original);
        const currentIndex = allRows.findIndex((r) => r.id === args.row.id);

        const index = currentIndex >= 0 ? currentIndex : 0;
        open({
            component: (
                <EtlErrorDetailsSheet
                    key={index}
                    error={args.row.original}
                    allErrors={allErrors}
                    initialIndex={index}
                />
            ),
            initialWidth: "40%",
            minWidth: "25%",
            maxWidth: "60%",
            isPinned: false,
        });
    };

    return (
        <Button variant="link" onClick={handleOpenSheet}>
            <Icon icon="preview" margin="m-0" />
        </Button>
    );
};

export const HyperLinkDocumentCellValue = ({ getValue }: Pick<CellContext<FlatError, unknown>, "getValue">) => {
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);

    if (!getValue()) {
        return <CellValue value="-" />;
    }

    return <CellDocumentValue value={getValue()} databaseName={dbName} hasHyperlinkForIds />;
};

export const CellTaskWrapper = ({ row }: CellContext<FlatError, string>) => {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { etlName, transformationName, taskId, etlType } = row.original;

    const taskLink =
        etlName && taskId != null && etlType != null ? getEtlEditLink(databaseName, taskId, etlType) : null;

    const isAiTask = AI_ONLY_TASK_TYPES.includes(etlType);
    const content = isAiTask ? (
        <>{etlName}</>
    ) : (
        <>
            {etlName}/{transformationName}
        </>
    );

    return <div className="cell-value">{taskLink ? <a href={taskLink}>{content}</a> : content}</div>;
};

export const CellTaskHealthWrapper = ({ getValue }: CellContext<FlatError, EtlHealthStatus | null>) => {
    const { bg, icon, label } = healthStatusToBadge(getValue());
    return (
        <PopoverWithHoverWrapper
            message={getPopoverMessageForTaskHealth(getValue())}
            wrapperClassName="d-flex align-items-center h-100"
            inline={false}
        >
            <Badge bg={bg} className="rounded-pill cell-value">
                <Icon icon={icon} />
                {label}
            </Badge>
        </PopoverWithHoverWrapper>
    );
};

export const CellDateWithRelativeTimeWrapper = ({ getValue }: CellContext<FlatError, string>) => {
    const rawValue = getValue();

    const dateValue = useMemo(() => {
        if (!rawValue) {
            return null;
        }
        const parsed = new Date(rawValue);
        return isNaN(parsed.getTime()) ? null : parsed;
    }, [rawValue]);

    if (!dateValue) {
        return <CellValue value="-" />;
    }

    return (
        <PopoverWithHoverWrapper
            message={
                <>
                    <b>UTC:</b> {moment(dateValue).utc().format(genUtils.dateFormat)}
                </>
            }
        >
            <small className="vstack cell-value value-string">
                <span>{moment(dateValue).format(genUtils.dateFormat)}</span>
                <small>{moment(dateValue).fromNow()}</small>
            </small>
        </PopoverWithHoverWrapper>
    );
};

export const CellAffectedDocumentsWrapper = ({ getValue }: CellContext<FlatError, number>) => {
    return <CellValue value={getValue()} />;
};

export const CellEtlTypeWrapper = ({ getValue }: CellContext<FlatError, StudioEtlType>) => {
    const icon = getEtlTypeIcon(getValue());
    const label = getEtlTypeLabel(getValue());
    return (
        <div className="cell-value value-string">
            <Icon icon={icon} />
            <CellValue value={label} />
        </div>
    );
};
