import { DatabaseLocalInfo, DatabaseSharedInfo, TopLevelDatabaseInfo } from "components/models/databases";
import { RichPanelDetailItem, RichPanelDetails } from "components/common/RichPanel";
import React, { useState } from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { sumBy } from "lodash";
import genUtils from "common/generalUtils";
import { withPreventDefault } from "components/utils/common";
import DatabaseUtils from "components/utils/DatabaseUtils";
import BackupInfo = Raven.Client.ServerWide.Operations.BackupInfo;
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { Badge, Button } from "reactstrap";
import { Icon } from "components/common/Icon";
import {
    selectDatabaseState,
    selectTopLevelState,
} from "components/pages/resources/databases/store/databasesViewSelectors";
import { openNotificationCenterForDatabase } from "components/pages/resources/databases/store/databasesViewActions";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { useAppUrls } from "components/hooks/useAppUrls";
import AlertsPopover from "./ValidDatabasePropertiesPanelAlertsPopover";
import PerfHintsPopover from "./ValidDatabasePropertiesPanelPerfHintsPopover";
import "./ValidDatabasePropertiesPanel.scss";
import { locationAwareLoadableData } from "components/models/common";

export interface ValidDatabasePropertiesPanelPopoverProps {
    isCurrentNodeRelevant: boolean;
    localNodeTag: string;
    localTotal: number;
    remoteTopLevelStates: TopLevelDatabaseInfo[];
    getServerNodeUrl: (nodeTag: string) => string;
    openNotificationCenter: () => void;
}

interface ValidDatabasePropertiesPanelProps {
    db: DatabaseSharedInfo;
    panelCollapsed: boolean;
    togglePanelCollapsed: () => void;
}

export function ValidDatabasePropertiesPanel(props: ValidDatabasePropertiesPanelProps) {
    const { db, panelCollapsed, togglePanelCollapsed } = props;

    const dbState = useAppSelector(selectDatabaseState(db.name));
    const topLevelState = useAppSelector(selectTopLevelState(db.name));

    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const allNodes = useAppSelector(clusterSelectors.allNodes);
    const isCurrentNodeRelevant = db.currentNode.isRelevant;

    const dispatch = useAppDispatch();
    const { appUrl } = useAppUrls();

    const nonEmptyDbState = dbState
        .filter((x) => x.status === "success" && !x.data.loadError)
        .map((x) => x.data)
        .filter((x) => x);

    const nonEmptyTopLevelState = topLevelState
        .filter((x) => x.status === "success" && !x.data.loadError)
        .map((x) => x.data)
        .filter((x) => x);

    const indexingErrors = sumBy(nonEmptyDbState, (x) => x?.indexingErrors ?? 0);
    const alertsCount = sumBy(nonEmptyTopLevelState, (x) => x?.alerts ?? 0);
    const performanceHintsCount = sumBy(nonEmptyTopLevelState, (x) => x?.performanceHints ?? 0);
    const indexingPaused = nonEmptyDbState.some((x) => x?.indexingStatus === "Paused");
    const indexingDisabled = nonEmptyDbState.some((x) => x?.indexingStatus === "Disabled");

    const localAlertsCount = nonEmptyTopLevelState.find((x) => x.nodeTag === localNodeTag)?.alerts ?? 0;
    const localPerformanceHintsCount =
        nonEmptyTopLevelState.find((x) => x.nodeTag === localNodeTag)?.performanceHints ?? 0;

    const hasRemoteAlerts = alertsCount > localAlertsCount;
    const hasRemotePerformanceHints = performanceHintsCount > localPerformanceHintsCount;

    const remoteTopLevelStates = nonEmptyTopLevelState.filter((x) => x.nodeTag !== localNodeTag);

    const maxSizes = genUtils.maxByShard(
        nonEmptyDbState,
        (x) => x.location.shardNumber,
        (x) => (x.totalSize?.SizeInBytes ?? 0) + (x.tempBuffersSize?.SizeInBytes ?? 0)
    );
    const totalSize = sumBy(maxSizes);

    const totalDocuments = sumBy(
        genUtils.maxByShard(
            nonEmptyDbState,
            (x) => x.location.shardNumber,
            (x) => x.documentsCount
        )
    );

    const hasAnyLoadError = dbState.some((x) => x.data?.loadError);
    const isGeneralInfoVisible = getIsGeneralInfoVisible(dbState, db.isSharded);

    const localDocumentsUrl = appUrl.forDocuments(null, db.name);
    const documentsUrl = db.currentNode.isRelevant
        ? localDocumentsUrl
        : appUrl.toExternalDatabaseUrl(db, localDocumentsUrl);

    const localIndexingErrorsUrl = appUrl.forIndexErrors(db.name);
    const indexingErrorsUrl = db.currentNode.isRelevant
        ? localIndexingErrorsUrl
        : appUrl.toExternalDatabaseUrl(db, localIndexingErrorsUrl);

    const localIndexingListUrl = appUrl.forIndexes(db.name);
    const indexingListUrl = db.currentNode.isRelevant
        ? localIndexingListUrl
        : appUrl.toExternalDatabaseUrl(db, localIndexingListUrl);

    const localStorageReportUrl = appUrl.forStatusStorageReport(db.name);
    const storageReportUrl = db.currentNode.isRelevant
        ? localStorageReportUrl
        : appUrl.toExternalDatabaseUrl(db, localStorageReportUrl);

    const localBackupUrl = appUrl.forBackups(db.name);
    const backupUrl = db.currentNode.isRelevant ? localBackupUrl : appUrl.toExternalDatabaseUrl(db, localBackupUrl);

    const linksTarget = db.currentNode.isRelevant ? undefined : "_blank";

    const backupInfo = findLatestBackup(nonEmptyDbState);
    const backupStatus = DatabaseUtils.computeBackupStatus(backupInfo);

    const [perfHintsPopoverElement, setPerfHintsPopoverElement] = useState<HTMLElement>();
    const [alertsPopoverElement, setAlertsPopoverElement] = useState<HTMLElement>();

    const getServerNodeUrl = (nodeTag: string): string => {
        const serverUrl = allNodes.find((node) => node.nodeTag === nodeTag)?.serverUrl;
        if (!serverUrl) {
            return "#";
        }
        return serverUrl + "/studio/index.html#databases";
    };

    const alertSection = (
        <React.Fragment>
            <Icon icon="warning" />
            {!isCurrentNodeRelevant ? "Alerts" : `${localAlertsCount} ${localAlertsCount === 1 ? "Alert" : "Alerts"}`}
        </React.Fragment>
    );

    const performanceHintsSection = (
        <React.Fragment>
            <Icon icon="rocket" />
            {!isCurrentNodeRelevant
                ? "Performance hints"
                : `${localPerformanceHintsCount} Performance ${localPerformanceHintsCount === 1 ? " hint" : "hints"}`}
        </React.Fragment>
    );

    return (
        <RichPanelDetails className="flex-wrap pb-1">
            <RichPanelDetailItem>
                <Button
                    color="secondary"
                    title={panelCollapsed ? "Expand distribution details" : "Collapse distribution details"}
                    onClick={togglePanelCollapsed}
                    className="ms-1 btn-toggle-panel rounded-pill"
                >
                    <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                </Button>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <div className="encryption">
                    {db.isEncrypted && (
                        <span title="This database is encrypted">
                            <Icon icon="key" color="success" margin="m-0" />
                        </span>
                    )}
                    {!db.isEncrypted && (
                        <span title="This database is not encrypted">
                            <Icon icon="unencrypted" color="muted" margin="m-0" />
                        </span>
                    )}
                </div>
            </RichPanelDetailItem>

            {isGeneralInfoVisible && (
                <>
                    <RichPanelDetailItem>
                        <a href={storageReportUrl} target={linksTarget}>
                            <Icon icon="drive" /> {genUtils.formatBytesToSize(totalSize)}
                        </a>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem>
                        <a href={documentsUrl} target={linksTarget}>
                            <Icon icon="documents" /> {totalDocuments.toLocaleString()}
                        </a>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem>
                        <a href={indexingListUrl} target={linksTarget}>
                            <Icon icon="index" /> {db.indexesCount}
                        </a>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem title="Click to navigate to Backups view" className="text-danger">
                        <a href={backupUrl} target={linksTarget} className={"text-" + backupStatus.color}>
                            <Icon icon="backup" />
                            {backupStatus.text}
                        </a>
                    </RichPanelDetailItem>
                </>
            )}

            <div className="rich-panel-details-right">
                {indexingErrors > 0 && (
                    <RichPanelDetailItem
                        key="indexing-errors"
                        title="Indexing errors. Click to view the Indexing Errors."
                    >
                        <Badge color="faded-danger" className="d-flex align-items-center lh-base rounded-pill">
                            <a href={indexingErrorsUrl} target={linksTarget} className="no-decor">
                                <Icon icon="exclamation" /> {indexingErrors} Indexing errors
                            </a>
                        </Badge>
                    </RichPanelDetailItem>
                )}
                {indexingPaused && (
                    <RichPanelDetailItem
                        key="indexing-paused"
                        title="Indexing is paused. Click to view the Index List."
                    >
                        <Badge color="faded-warning" className="d-flex align-items-center lh-base rounded-pill">
                            <a href={indexingListUrl} target={linksTarget} className="no-decor">
                                <Icon icon="pause" /> Indexing paused
                            </a>
                        </Badge>
                    </RichPanelDetailItem>
                )}
                {indexingDisabled && (
                    <RichPanelDetailItem key="indexing-disabled" title="Indexing is disabled">
                        <Badge color="faded-warning" className="d-flex align-items-center lh-base rounded-pill">
                            <a href={indexingListUrl} target={linksTarget} className="no-decor">
                                <Icon icon="stop" /> Indexing disabled
                            </a>
                        </Badge>
                    </RichPanelDetailItem>
                )}
                {alertsCount > 0 && (
                    <>
                        <RichPanelDetailItem
                            key="alerts"
                            title={db.currentNode.isRelevant ? "Click to view alerts in Notification Center" : ""}
                        >
                            <Badge color="faded-warning" className="d-flex align-items-center lh-base rounded-pill">
                                <a
                                    href="#"
                                    onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}
                                    className="no-decor"
                                    ref={setAlertsPopoverElement}
                                >
                                    {alertSection}
                                    {hasRemoteAlerts && (
                                        <>
                                            <div className="vr bg-warning"></div>
                                            <Icon icon="global" />
                                            {alertsCount}
                                        </>
                                    )}
                                </a>
                            </Badge>
                        </RichPanelDetailItem>
                        {hasRemoteAlerts && (
                            <PopoverWithHover target={alertsPopoverElement}>
                                <AlertsPopover
                                    isCurrentNodeRelevant={isCurrentNodeRelevant}
                                    localNodeTag={localNodeTag}
                                    localTotal={localAlertsCount}
                                    getServerNodeUrl={getServerNodeUrl}
                                    openNotificationCenter={() => dispatch(openNotificationCenterForDatabase(db))}
                                    remoteTopLevelStates={remoteTopLevelStates}
                                />
                            </PopoverWithHover>
                        )}
                    </>
                )}
                {performanceHintsCount > 0 && (
                    <>
                        <RichPanelDetailItem
                            key="performance-hints"
                            title={
                                db.currentNode.isRelevant
                                    ? "Click to view performance hints in Notification Center"
                                    : ""
                            }
                        >
                            <Badge color="faded-info" className="d-flex align-items-center lh-base rounded-pill">
                                <a
                                    href="#"
                                    onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}
                                    className="no-decor"
                                    ref={setPerfHintsPopoverElement}
                                >
                                    {performanceHintsSection}
                                    {hasRemotePerformanceHints && (
                                        <>
                                            <div className="vr bg-info"></div>
                                            <Icon icon="global" />
                                            {performanceHintsCount}
                                        </>
                                    )}
                                </a>
                            </Badge>
                        </RichPanelDetailItem>
                        {hasRemotePerformanceHints && (
                            <PopoverWithHover target={perfHintsPopoverElement}>
                                <PerfHintsPopover
                                    isCurrentNodeRelevant={isCurrentNodeRelevant}
                                    localNodeTag={localNodeTag}
                                    localTotal={localPerformanceHintsCount}
                                    getServerNodeUrl={getServerNodeUrl}
                                    openNotificationCenter={() => dispatch(openNotificationCenterForDatabase(db))}
                                    remoteTopLevelStates={remoteTopLevelStates}
                                />
                            </PopoverWithHover>
                        )}
                    </>
                )}
                {hasAnyLoadError && (
                    <RichPanelDetailItem key="load-error">
                        <Badge color="faded-danger" className="d-flex align-items-center lh-base rounded-pill pulse">
                            <Icon icon="danger" /> Database has load errors!
                        </Badge>
                    </RichPanelDetailItem>
                )}
            </div>
        </RichPanelDetails>
    );
}

function findLatestBackup(localInfos: DatabaseLocalInfo[]): BackupInfo {
    const nonEmptyBackups = localInfos.filter((x) => x.backupInfo && x.backupInfo.LastBackup);
    if (nonEmptyBackups.length === 0) {
        return null;
    }

    const backupInfos = nonEmptyBackups.map((x) => x.backupInfo);
    backupInfos.sort((a, b) => -1 * a.LastBackup.localeCompare(b.LastBackup));

    return backupInfos[0];
}

function getIsGeneralInfoVisible(
    dbStates: locationAwareLoadableData<DatabaseLocalInfo>[],
    isSharded: boolean
): boolean {
    if (isSharded) {
        const shardNumbers = [...new Set(dbStates.map((x) => x.location?.shardNumber))];

        return shardNumbers.every((shardNumber) => {
            const stateForShard = dbStates.filter((x) => x.location?.shardNumber === shardNumber);
            return getIsSuccessWithNoLoadError(stateForShard);
        });
    }

    return getIsSuccessWithNoLoadError(dbStates);
}

function getIsSuccessWithNoLoadError(dbStates: locationAwareLoadableData<DatabaseLocalInfo>[]) {
    if (dbStates.some((x) => x.status === "success" && !x.data?.loadError)) {
        return true;
    }

    return false;
}

export const exportedForTesting = {
    findLatestBackup,
    getIsGeneralInfoVisible,
};
