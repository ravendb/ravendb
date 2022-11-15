import React, { useCallback, useState } from "react";
import { useAppUrls } from "hooks/useAppUrls";
import classNames from "classnames";
import { withPreventDefault } from "../../../../utils/common";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import {
    Button,
    ButtonGroup,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Spinner,
    UncontrolledDropdown,
} from "reactstrap";

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

    const [globalLockChanges] = useState(false);
    // TODO: IDK I just wanted it to compile

    return (
        <div className="indexesToolbar-actions flex-horizontal">
            <div
                className={classNames("btn-group-label margin-right flex-horizontal", {
                    active: selectedIndexes.length > 0,
                })}
                data-label="Selection"
            >
                <Button
                    color="danger"
                    disabled={selectedIndexes.length === 0}
                    onClick={deleteSelectedIndexes}
                    className="margin-right-xxs"
                >
                    <i className="icon-trash" />
                    <span>Delete</span>
                </Button>
                <UncontrolledDropdown>
                    <DropdownToggle
                        className="margin-right-xxs"
                        title="Set the indexing state for the selected indexes"
                        disabled={selectedIndexes.length === 0}
                        data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                    >
                        {globalLockChanges && <Spinner size="sm" className="margin-right-xs" />}
                        {!globalLockChanges && <i className="icon-play" />}
                        <span>Set indexing state...</span>
                    </DropdownToggle>

                    <DropdownMenu>
                        <DropdownItem onClick={withPreventDefault(enableSelectedIndexes)} title="Enable indexing">
                            <i className="icon-play" /> <span>Enable</span>
                        </DropdownItem>
                        <DropdownItem onClick={withPreventDefault(disableSelectedIndexes)} title="Disable indexing">
                            <i className="icon-cancel text-danger" /> <span>Disable</span>
                        </DropdownItem>
                        <DropdownItem divider />
                        <DropdownItem onClick={withPreventDefault(resumeSelectedIndexes)} title="Resume indexing">
                            <i className="icon-play" /> <span>Resume</span>
                        </DropdownItem>
                        <DropdownItem onClick={withPreventDefault(pauseSelectedIndexes)} title="Pause indexing">
                            <i className="icon-pause text-warning" /> <span>Pause</span>
                        </DropdownItem>
                    </DropdownMenu>
                </UncontrolledDropdown>

                <UncontrolledDropdown>
                    <DropdownToggle
                        title="Set the lock mode for the selected indexes"
                        disabled={selectedIndexes.length === 0}
                        data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                    >
                        {globalLockChanges && <Spinner size="sm" className="margin-right-xs" />}
                        {!globalLockChanges && <i className="icon-lock" />}
                        <span>Set lock mode...</span>
                    </DropdownToggle>

                    <DropdownMenu>
                        <DropdownItem onClick={unlockSelectedIndexes} title="Unlock selected indexes">
                            <i className="icon-unlock" /> <span>Unlock</span>
                        </DropdownItem>
                        <DropdownItem onClick={lockSelectedIndexes} title="Lock selected indexes">
                            <i className="icon-lock" /> <span>Lock</span>
                        </DropdownItem>
                        <DropdownItem divider />
                        <DropdownItem onClick={lockErrorSelectedIndexes} title="Lock (Error) selected indexes">
                            <i className="icon-lock-error" /> <span>Lock (Error)</span>
                        </DropdownItem>
                    </DropdownMenu>
                </UncontrolledDropdown>
            </div>

            <Button color="primary" href={newIndexUrl}>
                <i className="icon-plus" />
                <span>New index</span>
            </Button>
        </div>
    );
}
