import { DatabaseLocalInfo, DatabaseSharedInfo } from "components/models/databases";
import { RichPanelDetailItem, RichPanelDetails } from "components/common/RichPanel";
import React from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { openNotificationCenterForDatabase, selectDatabaseState } from "components/common/shell/databasesSlice";
import { sumBy } from "lodash";
import genUtils from "common/generalUtils";
import appUrl from "common/appUrl";
import { withPreventDefault } from "components/utils/common";
import DatabaseUtils from "components/utils/DatabaseUtils";
import BackupInfo = Raven.Client.ServerWide.Operations.BackupInfo;

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

    const dispatch = useAppDispatch();

    const nonEmptyDbState = dbState
        .filter((x) => x.status === "success" && !x.data.loadError)
        .map((x) => x.data)
        .filter((x) => x);

    const indexingErrors = sumBy(nonEmptyDbState, (x) => x?.indexingErrors ?? 0);
    const alerts = sumBy(nonEmptyDbState, (x) => x?.alerts ?? 0);
    const performanceHints = sumBy(nonEmptyDbState, (x) => x?.performanceHints ?? 0);
    const indexingPaused = nonEmptyDbState.some((x) => x?.indexingStatus === "Paused");
    const indexingDisabled = nonEmptyDbState.some((x) => x?.indexingStatus === "Disabled");

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

    return (
        <RichPanelDetails className="flex-wrap pb-1">
            <RichPanelDetailItem>
                <div className="encryption">
                    {db.encrypted && (
                        <span title="This database is encrypted">
                            <i className="icon-key text-success" />
                        </span>
                    )}
                    {!db.encrypted && (
                        <span title="This database is not encrypted">
                            <i className="icon-unencrypted text-muted" />
                        </span>
                    )}
                </div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <a href={storageReportUrl} target={linksTarget}>
                    <i className="icon-drive me-1" /> {genUtils.formatBytesToSize(totalSize)}
                </a>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <a href={documentsUrl} target={linksTarget}>
                    <i className="icon-documents me-1" /> {totalDocuments.toLocaleString()}
                </a>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <a href={indexingListUrl} target={linksTarget}>
                    <i className="icon-index me-1" /> {db.indexesCount}
                </a>
            </RichPanelDetailItem>
            <RichPanelDetailItem title="Click to navigate to Backups view" className="text-danger">
                <a href={backupUrl} target={linksTarget}>
                    <i className="icon-backup me-1" />{" "}
                    <span className={"text-" + backupStatus.color}>{backupStatus.text}</span>
                </a>
            </RichPanelDetailItem>

            <div className="rich-panel-details-right">
                {indexingErrors > 0 && (
                    <RichPanelDetailItem
                        key="indexing-errors"
                        title="Indexing errors. Click to view the Indexing Errors."
                        className="text-danger"
                    >
                        <a href={indexingErrorsUrl} target={linksTarget}>
                            <i className="icon-exclamation me-1" /> {indexingErrors} Indexing errors
                        </a>
                    </RichPanelDetailItem>
                )}
                {indexingPaused && (
                    <RichPanelDetailItem
                        key="indexing-paused"
                        title="Indexing is paused. Click to view the Index List."
                        className="text-warning"
                    >
                        <a href={indexingListUrl} target={linksTarget}>
                            <i className="icon-pause me-1" /> Indexing paused
                        </a>
                    </RichPanelDetailItem>
                )}
                {indexingDisabled && (
                    <RichPanelDetailItem key="indexing-disabled" title="Indexing is disabled" className="text-danger">
                        <a href={indexingListUrl} target={linksTarget}>
                            <i className="icon-stop me-1" /> Indexing disabled
                        </a>
                    </RichPanelDetailItem>
                )}
                {alerts > 0 && (
                    <RichPanelDetailItem
                        key="alerts"
                        title="Click to view alerts in Notification Center"
                        className="text-warning"
                    >
                        {db.currentNode.relevant ? (
                            <a
                                href="#"
                                onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}
                            >
                                <i className="icon-warning me-1" /> {alerts.toLocaleString()} Alerts
                            </a>
                        ) : (
                            <>
                                <i className="icon-warning me-1" /> {alerts.toLocaleString()} Alerts
                            </>
                        )}
                    </RichPanelDetailItem>
                )}
                {performanceHints > 0 && (
                    <RichPanelDetailItem
                        key="performance-hints"
                        title="Click to view performance hints in Notification Center"
                        className="text-info"
                    >
                        {db.currentNode.relevant ? (
                            <a
                                href="#"
                                onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}
                            >
                                <i className="icon-rocket me-1" /> {performanceHints.toLocaleString()} Performance hints
                            </a>
                        ) : (
                            <>
                                <i className="icon-rocket me-1" /> {performanceHints.toLocaleString()} Performance hints
                            </>
                        )}
                    </RichPanelDetailItem>
                )}
                {hasAnyLoadError && (
                    <RichPanelDetailItem key="load-error" className="text-danger pulse">
                        <i className="icon-danger me-1" /> Database has load errors!
                    </RichPanelDetailItem>
                )}
            </div>
        </RichPanelDetails>
    );
}
