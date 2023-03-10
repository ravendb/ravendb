import { DatabaseSharedInfo } from "components/models/databases";
import { RichPanelDetailItem, RichPanelDetails } from "components/common/RichPanel";
import React from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { openNotificationCenterForDatabase, selectDatabaseState } from "components/common/shell/databasesSlice";
import { sumBy } from "lodash";
import genUtils from "common/generalUtils";
import appUrl from "common/appUrl";
import notificationCenter from "common/notifications/notificationCenter";
import { withPreventDefault } from "components/utils/common";

interface ValidDatabasePropertiesPanelProps {
    db: DatabaseSharedInfo;
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
        (x) => x.totalSize.SizeInBytes + x.tempBuffersSize.SizeInBytes
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

    //TODO: global backup status

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

    const linksTarget = db.currentNode.relevant ? undefined : "_blank";

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
                <i className="icon-drive me-1" /> {genUtils.formatBytesToSize(totalSize)}
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
            {/* TODO
            <RichPanelDetailItem title="Last backup" className="text-danger">
                <i className="icon-backup me-1" /> TODO
            </RichPanelDetailItem>
            */}

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
                        <a href="#" onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}>
                            <i className="icon-warning me-1" /> {alerts} Alerts
                        </a>
                    </RichPanelDetailItem>
                )}
                {performanceHints > 0 && (
                    <RichPanelDetailItem
                        key="performance-hints"
                        title="Click to view performance hints in Notification Center"
                        className="text-info"
                    >
                        <a href="#" onClick={withPreventDefault(() => dispatch(openNotificationCenterForDatabase(db)))}>
                            <i className="icon-rocket me-1" /> {performanceHints} Performance hints
                        </a>
                    </RichPanelDetailItem>
                )}
                {hasAnyLoadError && (
                    <RichPanelDetailItem key="load-error" className="text-danger pulse">
                        <i className="icon-danger me-1" /> Database has load errors!
                    </RichPanelDetailItem>
                )}
            </div>

            {/* TODO <div data-bind="if: databaseAccessText">
                            <div className="database-access" title="Database access level">
                                <i data-bind="attr: { class: databaseAccessColor() + ' ' + databaseAccessClass() }"/>
                                <small data-bind="text: databaseAccessText"/>
                            </div>
                        </div>
                        <div className="storage">
                            <small><i className="icon-drive"/></small>
                            <a className="set-size" data-toggle="size-tooltip"
                               data-bind="attr: { href: $root.storageReportUrl($data) }, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }">
                                <small
    data-bind="text: $root.formatBytes(totalSize() + totalTempBuffersSize())"/>
                            </a>
                        </div>
                 
                        <!--ko if: !uptime()-->
                        <div className="uptime text-muted">
                            <small><i className="icon-recent"/></small>
                            <small>Offline</small>
                        </div>
                        <!--/ko-->
                        <!--ko if: uptime()-->
                        <div className="uptime">
                            <small><i className="icon-recent"/></small>
                            <span title="The database uptime">
                        <small className="hidden-compact">Up for</small>
                        <small data-bind="text: uptime()"/>
                    </span>
                        </div>
                        <!--/ko-->
                        <div className="backup">
                            <div className="properties-value value-only">
                                <a className="set-size" title="Click to navigate to Backups view"
                                   data-bind="css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }, attr: { href: $root.backupsViewUrl($data), class: backupStatus() }">
                                    <small><i className="icon-backup"/></small>
                                    <small data-bind="text: lastBackupText"/>
                                </a>
                            </div>
                        </div>*/}
        </RichPanelDetails>
    );
}
