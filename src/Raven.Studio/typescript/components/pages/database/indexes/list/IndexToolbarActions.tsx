import React, { useCallback } from "react";
import { useAppUrls } from "hooks/useAppUrls";
import classNames from "classnames";
import { withPreventDefault } from "../../../../utils/common";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;

interface IndexToolbarActionProps {
    selectedIndexes: string[];
    deleteSelectedIndexes: () => Promise<void>;
    enableSelectedIndexes: () => Promise<void>;
    disableSelectedIndexes: () => Promise<void>;
    pauseSelectedIndexes: () => Promise<void>;
    resumeSelectedIndexes: () => Promise<void>;
    setLockModeSelectedIndexes: (lockMode: IndexLockMode) => Promise<void>;
}

export default function IndexToolbarAction(props: IndexToolbarActionProps) {
    const { forCurrentDatabase: urls } = useAppUrls();
    const newIndexUrl = urls.newIndex();

    const {
        selectedIndexes,
        deleteSelectedIndexes,
        enableSelectedIndexes,
        disableSelectedIndexes,
        pauseSelectedIndexes,
        resumeSelectedIndexes,
        setLockModeSelectedIndexes,
    } = props;

    const unlockSelectedIndexes = useCallback(
        async (e: React.MouseEvent<HTMLElement>) => {
            e.preventDefault();
            await setLockModeSelectedIndexes("Unlock");
        },
        [setLockModeSelectedIndexes]
    );

    const lockSelectedIndexes = useCallback(
        async (e: React.MouseEvent<HTMLElement>) => {
            e.preventDefault();
            await setLockModeSelectedIndexes("LockedIgnore");
        },
        [setLockModeSelectedIndexes]
    );

    const lockErrorSelectedIndexes = useCallback(
        async (e: React.MouseEvent<HTMLElement>) => {
            e.preventDefault();
            await setLockModeSelectedIndexes("LockedError");
        },
        [setLockModeSelectedIndexes]
    );

    return (
        <div className="indexesToolbar-actions">
            <div
                className={classNames("btn-group-label", { active: selectedIndexes.length > 0 })}
                data-label="Selection"
                role="group"
            >
                <button
                    type="button"
                    className="btn btn-danger"
                    disabled={selectedIndexes.length === 0}
                    onClick={deleteSelectedIndexes}
                >
                    <i className="icon-trash" />
                    <span>Delete</span>
                </button>
                <div className="btn-group">
                    <button
                        type="button"
                        className="btn btn-default dropdown-toggle"
                        title="Set the indexing state for the selected indexes"
                        disabled={selectedIndexes.length === 0}
                        data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges(),
                                                       css: { 'btn-spinner': spinners.globalLockChanges() }"
                        data-toggle="dropdown"
                        aria-haspopup="true"
                        aria-expanded="false"
                    >
                        <i className="icon-play" />
                        <span>Set indexing state...</span>
                        <span className="caret" />
                        <span className="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul className="dropdown-menu">
                        <li title="Enable indexing">
                            <a href="#" onClick={withPreventDefault(enableSelectedIndexes)}>
                                <i className="icon-play" />
                                <span>Enable</span>
                            </a>
                        </li>
                        <li title="Disable indexing">
                            <a href="#" onClick={withPreventDefault(disableSelectedIndexes)}>
                                <i className="icon-cancel" />
                                <span>Disable</span>
                            </a>
                        </li>
                        <li className="divider" />
                        <li title="Resume indexing">
                            <a href="#" onClick={withPreventDefault(resumeSelectedIndexes)}>
                                <i className="icon-play" />
                                <span>Resume</span>
                            </a>
                        </li>
                        <li title="Pause indexing">
                            <a href="#" onClick={withPreventDefault(pauseSelectedIndexes)}>
                                <i className="icon-pause" />
                                <span>Pause</span>
                            </a>
                        </li>
                    </ul>
                </div>
                <div className="btn-group">
                    <button
                        type="button"
                        className="btn btn-default dropdown-toggle"
                        disabled={selectedIndexes.length === 0}
                        title="Set the lock mode for the selected indexes"
                        data-bind="enable: selectedIndexesName().length && !spinners.globalLockChanges(), 
                                                       css: { 'btn-spinner': spinners.globalLockChanges() }"
                        data-toggle="dropdown"
                        aria-haspopup="true"
                        aria-expanded="false"
                    >
                        <i className="icon-lock" />
                        <span>Set lock mode...</span>
                        <span className="caret" />
                        <span className="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul className="dropdown-menu">
                        <li>
                            <a href="#" title="Unlock selected indexes" onClick={unlockSelectedIndexes}>
                                <i className="icon-unlock" />
                                <span>Unlock</span>
                            </a>
                        </li>
                        <li>
                            <a href="#" title="Lock selected indexes" onClick={lockSelectedIndexes}>
                                <i className="icon-lock" />
                                <span>Lock</span>
                            </a>
                        </li>
                        <li>
                            <a href="#" title="Lock (Error) selected indexes" onClick={lockErrorSelectedIndexes}>
                                <i className="icon-lock-error" />
                                <span>Lock (Error)</span>
                            </a>
                        </li>
                    </ul>
                </div>
            </div>
            <a className="btn btn-primary" href={newIndexUrl}>
                <i className="icon-plus" />
                <span>New index</span>
            </a>
        </div>
    );
}
