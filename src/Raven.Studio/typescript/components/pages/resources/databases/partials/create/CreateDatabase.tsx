import React, { useState } from "react";
import { Modal } from "reactstrap";
import CreateDatabaseRegular from "./regular/CreateDatabaseRegular";
import "./CreateDatabase.scss";
import CreateDatabaseFromBackup from "./formBackup/CreateDatabaseFromBackup";

type CreateMode = "regular" | "fromBackup";

interface CreateDatabaseProps {
    closeModal: () => void;
}

export default function CreateDatabase({ closeModal }: CreateDatabaseProps) {
    const [createMode, setCreateMode] = useState<CreateMode>("fromBackup");

    return (
        <Modal isOpen wrapClassName="bs5" size="lg" centered autoFocus fade className="create-database">
            {createMode === "regular" && (
                <CreateDatabaseRegular
                    closeModal={closeModal}
                    changeCreateModeToBackup={() => setCreateMode("fromBackup")}
                />
            )}
            {createMode === "fromBackup" && (
                <CreateDatabaseFromBackup
                    closeModal={closeModal}
                    changeCreateModeToRegular={() => setCreateMode("regular")}
                />
            )}

            {/* TODO remove? */}
            <div id="PopoverContainer" className="popover-container-fix"></div>
        </Modal>
    );
}
