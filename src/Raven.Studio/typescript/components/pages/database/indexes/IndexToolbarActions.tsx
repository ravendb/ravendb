import React from "react";
import { useAppUrls } from "../../../hooks/useAppUrls";

export default function IndexToolbarAction() {
    const urls = useAppUrls();
    const newIndexUrl = urls.newIndex();
    
    return (
        <div className="indexesToolbar-actions" data-bind="requiredAccess: 'DatabaseReadWrite'">
            <div className="btn-group-label" data-bind="css: { active: selectedIndexesName().length }"
                 data-label="Selection" role="group">
                <button className="btn btn-danger"
                        data-bind="click: deleteSelectedIndexes, enable: selectedIndexesName().length">
                    <i className="icon-trash"/><span>Delete</span>
                </button>
                <div className="btn-group">
                    <button type="button" className="btn btn-default dropdown-toggle"
                            title="Set the indexing state for the selected indexes"
                            data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges(),
                                                       css: { 'btn-spinner': spinners.globalLockChanges() }"
                            data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                        <i className="icon-play"/><span>Set indexing state...</span>
                        <span className="caret"/>
                        <span className="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul className="dropdown-menu">
                        <li data-bind="click: _.partial(enableSelectedIndexes, false),
                                                       attr: { title: 'Enable indexing on node ' + localNodeTag() }">
                            <a href="#">
                                <i className="icon-play"/>
                                <span>Enable</span>
                            </a>
                        </li>
                        <li data-bind="click: _.partial(disableSelectedIndexes, false),
                                                       attr: { title: 'Disable indexing on node ' + localNodeTag() }">
                            <a href="#">
                                <i className="icon-cancel"/>
                                <span>Disable</span>
                            </a>
                        </li>
                        <li className="divider"/>
                        <li data-bind="click: resumeSelectedIndexes,
                                                       attr: { title: 'Resume indexing on node ' + localNodeTag() }">
                            <a href="#">
                                <i className="icon-play"/>
                                <span>Resume</span>
                            </a>
                        </li>
                        <li data-bind="click: pauseSelectedIndexes,
                                                       attr: { title: 'Pause indexing on node ' + localNodeTag() }">
                            <a href="#">
                                <i className="icon-pause"/>
                                <span>Pause</span>
                            </a>
                        </li>
                    </ul>
                </div>
                <div className="btn-group">
                    <button type="button" className="btn btn-default dropdown-toggle"
                            title="Set the lock mode for the selected indexes"
                            data-bind="enable: selectedIndexesName().length && !spinners.globalLockChanges(), 
                                                       css: { 'btn-spinner': spinners.globalLockChanges() }"
                            data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                        <i className="icon-lock"/><span>Set lock mode...</span>
                        <span className="caret"/>
                        <span className="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul className="dropdown-menu">
                        <li data-bind="click: unlockSelectedIndexes">
                            <a href="#" title="Unlock selected indexes">
                                <i className="icon-unlock"/>
                                <span>Unlock</span>
                            </a>
                        </li>
                        <li data-bind="click: lockSelectedIndexes">
                            <a href="#" title="Lock selected indexes">
                                <i className="icon-lock"/>
                                <span>Lock</span>
                            </a>
                        </li>
                        <li data-bind="click: lockErrorSelectedIndexes">
                            <a href="#" title="Lock (Error) selected indexes">
                                <i className="icon-lock-error"/>
                                <span>Lock (Error)</span>
                            </a>
                        </li>
                    </ul>
                </div>
            </div>
            <a className="btn btn-primary" href={newIndexUrl}>
                <i className="icon-plus"/>
                <span>New index</span>
            </a>
        </div>
    )
}
