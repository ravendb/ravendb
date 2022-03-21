import React from "react";
import { useAppUrls } from "../../../hooks/useAppUrls";
import classNames from "classnames";
import { withPreventDefault } from "../../../utils/common";

interface IndexToolbarActionProps {
    selectedIndexes: string[];
    deleteSelectedIndexes: () => Promise<void>;
    enableSelectedIndexes: () => Promise<void>;
    disableSelectedIndexes: () => Promise<void>;
}

export default function IndexToolbarAction(props: IndexToolbarActionProps) {
    const urls = useAppUrls();
    const newIndexUrl = urls.newIndex();
    
    const { selectedIndexes, deleteSelectedIndexes, enableSelectedIndexes, disableSelectedIndexes } = props;
    
    return (
        <div className="indexesToolbar-actions" data-bind="requiredAccess: 'DatabaseReadWrite'">
            <div className={classNames("btn-group-label", { active: selectedIndexes.length > 0 })} data-label="Selection" role="group">
                <button type="button" className="btn btn-danger" disabled={selectedIndexes.length === 0} onClick={deleteSelectedIndexes}>
                    <i className="icon-trash"/><span>Delete</span>
                </button>
                <div className="btn-group">
                    <button type="button" className="btn btn-default dropdown-toggle"
                            title="Set the indexing state for the selected indexes"
                            disabled={selectedIndexes.length === 0}
                            data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges(),
                                                       css: { 'btn-spinner': spinners.globalLockChanges() }"
                            data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                        <i className="icon-play"/><span>Set indexing state...</span>
                        <span className="caret"/>
                        <span className="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul className="dropdown-menu">
                        <li title="Enable indexing">
                            <a href="#" onClick={withPreventDefault(enableSelectedIndexes)}>
                                <i className="icon-play"/>
                                <span>Enable</span>
                            </a>
                        </li>
                        <li title="Disable indexing">
                            <a href="#" onClick={withPreventDefault(disableSelectedIndexes)}>
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
                    <button type="button" className="btn btn-default dropdown-toggle" disabled={selectedIndexes.length === 0}
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
