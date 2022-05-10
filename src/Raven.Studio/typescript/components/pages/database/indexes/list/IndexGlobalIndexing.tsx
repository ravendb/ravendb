import React from "react";

export default function IndexGlobalIndexing() {
    return (
        <div className="pull-right" id="toggleIndexing" data-bind="requiredAccess: 'DatabaseReadWrite'">
            <button
                className="btn btn-default disable-indexing"
                title="Pause indexing process for ALL indexes until restart - Local node"
                data-bind="click: stopIndexing, visible: $root.globalIndexingStatus() !== 'Paused', 
                                       attr: { title: 'Pause indexing process for ALL indexes until restart - On node ' + localNodeTag() },
                                       enable: !spinners.globalStartStop() && $root.globalIndexingStatus() !== 'Disabled', css: { 'btn-spinner': spinners.globalStartStop() }"
            >
                <i className="icon-pause" />
                <span>Pause indexing until restart - Local node</span>
            </button>
            <button
                className="btn btn-success enable-indexing"
                title="Resume indexing process"
                data-bind="click: startIndexing, visible: $root.globalIndexingStatus() === 'Paused', enable: !spinners.globalStartStop(), css: { 'btn-spinner': spinners.globalStartStop() }"
            >
                <i className="icon-play" />
                <span>Resume indexing</span>
            </button>
        </div>
    );
}
