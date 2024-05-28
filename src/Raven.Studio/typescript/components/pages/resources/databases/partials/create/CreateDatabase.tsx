import React, { useState } from "react";
import { Modal } from "reactstrap";
import CreateDatabaseRegular from "./regular/CreateDatabaseRegular";
import CreateDatabaseFromBackup from "./formBackup/CreateDatabaseFromBackup";
import "./CreateDatabase.scss";

export type CreateDatabaseMode = "regular" | "fromBackup";

interface CreateDatabaseProps {
    closeModal: () => void;
    initialMode?: CreateDatabaseMode;
}

export default function CreateDatabase({ closeModal, initialMode }: CreateDatabaseProps) {
    const [createMode, setCreateMode] = useState<CreateDatabaseMode>(initialMode ?? "regular");

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
        </Modal>
    );
}
