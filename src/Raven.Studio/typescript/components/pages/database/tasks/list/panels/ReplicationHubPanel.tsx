import React from "react";
import { OngoingTaskReplicationHubInfo } from "../../../../../models/tasks";
import database from "models/resources/database";

interface ReplicationHubPanelProps {
    db: database;
    data: OngoingTaskReplicationHubInfo;
}

export function ReplicationHubPanel(props: ReplicationHubPanelProps) {
    //TODO: const canEdit = $root.isAdminAccessOrAbove() && !isServerWide();
    return <div>TEST</div>;
    //TODO
    return (
        <div className="panel destination-item pull-replication-hub">
            <div data-bind="attr: { 'data-state-text': $root.pluralize(ongoingHubs().length, 'sink', 'sinks'), class: 'state state-default' }"></div>
            <div className="padding-sm destination-info flex-vertical">
                <div className="flex-horizontal">TODO</div>
            </div>
            <div className="collapse panel-addon" data-bind="collapse: showDetails">
                <div className="padding-sm flex-horizontal" data-bind="visible: showDelayReplication">
                    <div className="flex-grow">
                        <div className="list-properties">
                            <div className="property-item" data-bind="visible: showDelayReplication">
                                <div className="property-name">Replication Delay Time:</div>
                                <div
                                    className="property-value"
                                    data-bind="text: delayHumane"
                                    title="Replication Delay Time"
                                ></div>
                            </div>
                        </div>
                    </div>
                </div>
                <div className="padding-sm" data-bind="visible: ongoingHubs().length">
                    <div data-bind="foreach: ongoingHubs">
                        <div className="panel destination-item external-replication">
                            <div data-bind="attr: { 'data-state-text': badgeText, class: 'state ' + badgeClass() }"></div>
                            <div className="padding-sm destination-info flex-vertical">
                                <div className="flex-horizontal">
                                    <div className="panel-name flex-grow">
                                        <h3 data-bind="text: taskName"></h3>
                                    </div>
                                    <div
                                        className="node"
                                        data-bind="template: { name: 'responsible-node-template' }"
                                    ></div>
                                </div>
                            </div>
                            <div className="panel-addon">
                                <div className="padding-sm">
                                    <div className="inline-properties">
                                        <div className="property-item">
                                            <div className="property-name">Task Status:</div>
                                            <div className="property-value" data-bind="text: badgeText"></div>
                                        </div>
                                        <div className="property-item">
                                            <div className="property-name">Sink Database:</div>
                                            <div
                                                className="property-value"
                                                data-bind="text: destinationDB"
                                                title="Destination database"
                                            ></div>
                                        </div>
                                        <div className="property-item">
                                            <div className="property-name">Actual Sink URL:</div>
                                            <div className="property-value" title="Actual Destination Url">
                                                <a
                                                    target="_blank"
                                                    data-bind="attr: { href: destinationURL() === 'N/A' ? null : destinationURL }, text: destinationURL()"
                                                ></a>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div className="panel-addon padding-sm" data-bind="visible: !ongoingHubs().length">
                    <h5 className="text-warning">
                        <i className="icon-empty-set"></i>
                        <span>No sinks connected</span>
                    </h5>
                </div>
            </div>
        </div>
    );
}
