import { DatabaseLocalInfo, DatabaseSharedInfo } from "components/models/databases";
import { RichPanelDetailItem, RichPanelDetails } from "components/common/RichPanel";
import React, { useState } from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { sumBy } from "lodash";
import genUtils from "common/generalUtils";
import appUrl from "common/appUrl";
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
import "./DatabasesPage.scss";
import { useAppUrls } from "components/hooks/useAppUrls";

interface ValidDatabasePropertiesPanelProps {
    db: DatabaseSharedInfo;
}

export function findLatestBackup(localInfos: DatabaseLocalInfo[]): BackupInfo {
    const nonEmptyBackups = localInfos.filter((x) => x.backupInfo && x.backupInfo.LastBackup);
    if (nonEmptyBackups.length === 0) {
        return null;
    }

    const backupInfos = nonEmptyBackups.map((x) => x.backupInfo);
    backupInfos.sort((a, b) => -1 * a.LastBackup.localeCompare(b.LastBackup));

    return backupInfos[0];
}

export function ValidDatabasePropertiesPanel(props: ValidDatabasePropertiesPanelProps) {
    const { db } = props;

    const dbState = useAppSelector(selectDatabaseState(db.name));
    const topLevelState = useAppSelector(selectTopLevelState(db.name));

    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const isCurrentNodeRelevant = db.currentNode.relevant;

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
    const alerts = sumBy(nonEmptyTopLevelState, (x) => x?.alerts ?? 0);
    const performanceHints = sumBy(nonEmptyTopLevelState, (x) => x?.performanceHints ?? 0);
    const indexingPaused = nonEmptyDbState.some((x) => x?.indexingStatus === "Paused");
    const indexingDisabled = nonEmptyDbState.some((x) => x?.indexingStatus === "Disabled");

    const localPerformanceHints = nonEmptyTopLevelState.find((x) => x.nodeTag === localNodeTag)?.performanceHints ?? 0;
    const localAlerts = nonEmptyTopLevelState.find((x) => x.nodeTag === localNodeTag)?.alerts ?? 0;

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

    const localDocumentsUrl = appUrl.forDocuments(null, db.name);
    const documentsUrl = db.currentNode.relevant
        ? localDocumentsUrl
        : appUrl.toExternalDatabaseUrl(db, localDocumentsUrl);

    const localIndexingErrorsUrl = appUrl.forIndexErrors(db);
    const indexingErrorsUrl = db.currentNode.relevant
        ? localIndexingErrorsUrl
        : appUrl.toExternalDatabaseUrl(db, localIndexingErrorsUrl);

    const localIndexingListUrl = appUrl.forIndexes(db);
    const indexingListUrl = db.currentNode.relevant
        ? localIndexingListUrl
        : appUrl.toExternalDatabaseUrl(db, localIndexingListUrl);

    const localStorageReportUrl = appUrl.forStatusStorageReport(db);
    const storageReportUrl = db.currentNode.relevant
        ? localStorageReportUrl
        : appUrl.toExternalDatabaseUrl(db, localStorageReportUrl);

    const localBackupUrl = appUrl.forBackups(db);
    const backupUrl = db.currentNode.relevant ? localBackupUrl : appUrl.toExternalDatabaseUrl(db, localBackupUrl);

    const linksTarget = db.currentNode.relevant ? undefined : "_blank";

    const backupInfo = findLatestBackup(nonEmptyDbState);
    const backupStatus = DatabaseUtils.computeBackupStatus(backupInfo);

    const [perfHintsPopoverElement, setPerfHintsPopoverElement] = useState<HTMLElement>();
    const [alertsPopoverElement, setAlertsPopoverElement] = useState<HTMLElement>();

    const alertSection = (
        <React.Fragment>
            <Icon icon="warning" />
            {!isCurrentNodeRelevant ? "Alerts" : `${localAlerts} ${localAlerts === 1 ? "Alert" : "Alerts"}`}
        </React.Fragment>
    );

    const performanceHintsSection = (
        <React.Fragment>
            <Icon icon="rocket" />
            {!isCurrentNodeRelevant
                ? "Performance hints"
                : `${localPerformanceHints} Performance ${localPerformanceHints === 1 ? " hint" : "hints"}`}
        </React.Fragment>
    );

    function PerfHintsPopover() {
        return (
            <div className="p-3 notifications-popover">
                {isCurrentNodeRelevant && (
                    <>
                        <strong className="d-block mb-1">Local</strong>
                        <div className="notifications-popover-grid">
                            <span>
                                <Icon icon="node" color="node" /> {localNodeTag}
                            </span>
                            <span>
                                <strong>{localPerformanceHints} </strong>
                                {localPerformanceHints === 1 ? "hint" : "hints"}
                            </span>
                            <Button
                                type="button"
                                size="xs"
                                color="info"
                                className="rounded-pill"
                                onClick={() => dispatch(openNotificationCenterForDatabase(db))}
                            >
                                <Icon icon="rocket" />
                                See {localPerformanceHints === 1 ? "hint" : "hints"}
                            </Button>
                        </div>
                    </>
                )}

                {remoteTopLevelStates.some((x) => !!x.performanceHints) && (
                    <>
                        {isCurrentNodeRelevant && <hr className="my-2" />}
                        <strong className="d-block mb-1">Remote</strong>

                        {remoteTopLevelStates.map((x) => {
                            if (!x.performanceHints) {
                                return null;
                            }

                            return (
                                <div key={x.nodeTag} className="notifications-popover-grid mb-1">
                                    <span>
                                        <Icon icon="node" color="node" /> {x.nodeTag}
                                    </span>
                                    <span>
                                        <strong>{x.performanceHints} </strong>
                                        {x.performanceHints === 1 ? "hint" : "hints"}
                                    </span>
                                    <Button type="button" size="xs" color="node" className="rounded-pill">
                                        <Icon icon="newtab" />
                                        Open node
                                    </Button>
                                </div>
                            );
                        })}
                    </>
                )}
            </div>
        );
    }

    function AlertsPopover() {
        return (
            <div className="p-3 notifications-popover">
                {isCurrentNodeRelevant && (
                    <>
                        <strong className="d-block mb-1">Local</strong>
                        <div className="notifications-popover-grid">
                            <span>
                                <Icon icon="node" color="node" /> {localNodeTag}
                            </span>
                            <span>
                                <strong>{localAlerts} </strong>
                                {localAlerts === 1 ? "alert" : "alerts"}
                            </span>
                            <Button
                                type="button"
                                size="xs"
                                color="warning"
                                className="rounded-pill"
                                onClick={() => dispatch(openNotificationCenterForDatabase(db))}
                            >
                                <Icon icon="alert" />
                                See {localAlerts === 1 ? "alert" : "alerts"}
                            </Button>
                        </div>
                    </>
                )}

                {remoteTopLevelStates.some((x) => !!x.performanceHints) && (
                    <>
                        {isCurrentNodeRelevant && <hr className="my-2" />}
                        <strong className="d-block mb-1">Remote</strong>

                        {remoteTopLevelStates.map((x) => {
                            if (!x.alerts) {
                                return null;
                            }

                            return (
                                <div key={x.nodeTag} className="notifications-popover-grid mb-1">
                                    <span>
                                        <Icon icon="node" color="node" /> {x.nodeTag}
                                    </span>
                                    <span>
                                        <strong>{x.alerts}</strong> {x.alerts === 1 ? "alert" : "alerts"}
                                    </span>
                                    <Button type="button" size="xs" color="node" className="rounded-pill">
                                        <Icon icon="newtab" />
                                        Open node
                                    </Button>
                                </div>
                            );
                        })}
                    </>
                )}
            </div>
        );
    }

    return (
        <RichPanelDetails className="flex-wrap pb-1">
            <RichPanelDetailItem>
                <div className="encryption">
                    {db.encrypted && (
                        <span title="This database is encrypted">
                            <Icon icon="key" color="success" margin="m-0" />
                        </span>
                    )}
                    {!db.encrypted && (
                        <span title="This database is not encrypted">
                            <Icon icon="unencrypted" color="muted" margin="m-0" />
                        </span>
                    )}
                </div>
            </RichPanelDetailItem>
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
                {alerts > 0 && (
                    <>
                        <RichPanelDetailItem key="alerts" title="Click to view alerts in Notification Center">
                            <Badge color="faded-warning" className="d-flex align-items-center lh-base rounded-pill">
                                <a
                                    href="#"
                                    onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}
                                    className="no-decor"
                                    ref={setAlertsPopoverElement}
                                >
                                    {alertSection}
                                    {!(isCurrentNodeRelevant && db.nodes.length === 1) && (
                                        <>
                                            <div className="vr bg-warning"></div>
                                            <Icon icon="global" />
                                            {alerts}
                                        </>
                                    )}
                                </a>
                            </Badge>
                        </RichPanelDetailItem>
                        <PopoverWithHover target={alertsPopoverElement}>
                            <AlertsPopover />
                        </PopoverWithHover>
                    </>
                )}
                {performanceHints > 0 && (
                    <>
                        <RichPanelDetailItem
                            key="performance-hints"
                            title="Click to view performance hints in Notification Center"
                        >
                            <Badge color="faded-info" className="d-flex align-items-center lh-base rounded-pill">
                                <a
                                    href="#"
                                    onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}
                                    className="no-decor"
                                    ref={setPerfHintsPopoverElement}
                                >
                                    {performanceHintsSection}
                                    {!(isCurrentNodeRelevant && db.nodes.length === 1) && (
                                        <>
                                            <div className="vr bg-info"></div>
                                            <Icon icon="global" />
                                            {performanceHints}
                                        </>
                                    )}
                                </a>
                            </Badge>
                        </RichPanelDetailItem>
                        <PopoverWithHover target={perfHintsPopoverElement}>
                            <PerfHintsPopover />
                        </PopoverWithHover>
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
