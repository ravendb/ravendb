import { Icon } from "components/common/Icon";
import React from "react";
import { Button } from "reactstrap";

export default function IndexGlobalIndexing() {
    return (
        <div className="pull-right" id="toggleIndexing" data-bind="requiredAccess: 'DatabaseReadWrite'">
            <Button
                className="disable-indexing"
                title="Pause indexing process for ALL indexes until restart - Local node"
                data-bind="click: stopIndexing, visible: $root.globalIndexingStatus() !== 'Paused', 
                                       attr: { title: 'Pause indexing process for ALL indexes until restart - On node ' + localNodeTag() },
                                       enable: !spinners.globalStartStop() && $root.globalIndexingStatus() !== 'Disabled', css: { 'btn-spinner': spinners.globalStartStop() }"
            >
                <Icon icon="pause" />
                Pause indexing until restart - Local node
            </Button>
            <Button
                color="success"
                className="enable-indexing"
                title="Resume indexing process"
                data-bind="click: startIndexing, visible: $root.globalIndexingStatus() === 'Paused', enable: !spinners.globalStartStop(), css: { 'btn-spinner': spinners.globalStartStop() }"
            >
                <Icon icon="play" />
                Resume indexing
            </Button>
        </div>
    );
}
