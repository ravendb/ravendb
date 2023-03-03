import { DatabaseSharedInfo } from "components/models/databases";
import { RichPanelDetailItem, RichPanelDetails } from "components/common/RichPanel";
import React from "react";
import { useAppSelector } from "components/store";
import { selectDatabaseState } from "components/common/shell/databasesSlice";
import { sumBy } from "lodash";
import genUtils from "common/generalUtils";
import appUrl from "common/appUrl";

interface ValidDatabasePropertiesPanelProps {
    db: DatabaseSharedInfo;
}

export function ValidDatabasePropertiesPanel(props: ValidDatabasePropertiesPanelProps) {
    const { db } = props;

    const dbState = useAppSelector(selectDatabaseState(db.name));

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

    return (
        <RichPanelDetails className="flex-wrap">
            <RichPanelDetailItem>
                <div className="encryption">
                    {db.encrypted && (
                        <small title="This database is encrypted">
                            <i className="icon-key text-success" />
                        </small>
                    )}
                    {!db.encrypted && (
                        <small title="This database is not encrypted">
                            <i className="icon-unencrypted text-muted" />
                        </small>
                    )}
                </div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <i className="icon-drive me-1" /> {genUtils.formatBytesToSize(totalSize)}
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <a href={documentsUrl} target={db.currentNode.relevant ? undefined : "_blank"}>
                    <i className="icon-documents me-1" /> {totalDocuments.toLocaleString()}
                </a>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <i className="icon-index me-1" /> {db.indexesCount}
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
                        <i className="icon-exclamation me-1" /> {indexingErrors} Indexing errors
                    </RichPanelDetailItem>
                )}
                {indexingPaused && (
                    <RichPanelDetailItem
                        key="indexing-paused"
                        title="Indexing is paused. Click to view the Index List."
                        className="text-warning"
                    >
                        <i className="icon-pause me-1" /> Indexing paused
                    </RichPanelDetailItem>
                )}
                {indexingDisabled && (
                    <RichPanelDetailItem key="indexing-disabled" title="Indexing is disabled" className="text-danger">
                        <i className="icon-stop me-1" /> Indexing disabled
                    </RichPanelDetailItem>
                )}
                {alerts > 0 && (
                    <RichPanelDetailItem
                        key="alerts"
                        title="Click to view alerts in Notification Center"
                        className="text-warning"
                    >
                        <i className="icon-warning me-1" /> {alerts} Alerts
                    </RichPanelDetailItem>
                )}
                {performanceHints > 0 && (
                    <RichPanelDetailItem
                        key="performance-hints"
                        title="Click to view alerts in Notification Center"
                        className="text-info"
                    >
                        <i className="icon-rocket me-1" /> {performanceHints} Performance hints
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
                        <div className="documents">
                            <small><i className="icon-document-group"/></small>
                            <a className="set-size" title="Number of documents. Click to view the Document List."
                               data-bind="attr: { href: $root.createAllDocumentsUrlObservable($data)}, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() },">
                                <small data-bind="text: (documentsCount() || 0).toLocaleString()"/>
                            </a>
                        </div>
                        <div className="indexes">
                            <small><i className="icon-index"/></small>
                            <a className="set-size" title="Number of indexes. Click to view the Index List."
                               data-bind="attr: { href: $root.indexesUrl($data) }, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }">
                                <small data-bind="text: (indexesCount() || 0).toLocaleString()"/>
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

            {/* TODO <div className="database-properties-right">
                        <div className="indexing-errors text-danger" data-bind="visible: indexingErrors()">
                            <small><i className="icon-exclamation"/></small>
                            <a className="set-size text-danger"
                               title="Indexing errors. Click to view the Indexing Errors."
                               data-bind="attr: { href: $root.indexErrorsUrl($data) }, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }">
                                <small data-bind="text: indexingErrors().toLocaleString()"/>
                                <small className="hidden-compact"
    data-bind="text: $root.pluralize(indexingErrors().toLocaleString(), 'indexing error', 'indexing errors', true)"/>
                            </a>
                        </div>
                        <div className="indexing-paused text-warning"
                             data-bind="visible: indexingPaused() && !indexingDisabled()">
                            <small><i className="icon-pause"/></small>
                            <a className="set-size text-warning"
                               title="Indexing is paused. Click to view the Index List."
                               data-bind="attr: { href: $root.indexesUrl($data) }">
                                <small>Indexing paused</small>
                            </a>
                        </div>
                       
                        <div className="alerts text-warning" data-bind="visible: alerts()">
                            <div className="set-size">
                                <small><i className="icon-warning"/></small>
                                <a className="set-size text-warning" title="Click to view alerts in Notification Center"
                                   href="#"
                                   data-bind="click: _.partial($root.openNotificationCenter, $data), css: { 'link-disabled': !canNavigateToDatabase() }">
                                    <small data-bind="text: alerts().toLocaleString()"/>
                                    <small
    data-bind="text: $root.pluralize(alerts().toLocaleString(), 'alert', 'alerts', true)"/>
                                </a>
                            </div>
                        </div>
                        <div className="performance-hints text-info" data-bind="visible: performanceHints()">
                            <div className="set-size">
                                <small><i className="icon-rocket"/></small>
                                <a className="set-size text-info" title="Click to view hints in Notification Center"
                                   href="#"
                                   data-bind="click: _.partial($root.openNotificationCenter, $data), css: { 'link-disabled': !canNavigateToDatabase() }">
                                    <small data-bind="text: performanceHints().toLocaleString()"/>
                                    <small className="hidden-compact"
    data-bind="text: $root.pluralize(performanceHints().toLocaleString(), 'performance hint', 'performance hints', true)"/>
                                </a>
                            </div>
                        </div>
                      
                    </div>*/}
        </RichPanelDetails>
    );
}
