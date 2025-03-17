import React, { useState } from "react";
import CreateDatabaseRegular from "./regular/CreateDatabaseRegular";
import CreateDatabaseFromBackup from "./formBackup/CreateDatabaseFromBackup";
import "./CreateDatabase.scss";
import Modal from "components/common/Modal";

export type CreateDatabaseMode = "regular" | "fromBackup";

interface CreateDatabaseProps {
    closeModal: () => void;
    initialMode?: CreateDatabaseMode;
}

export default function CreateDatabase({ closeModal, initialMode }: CreateDatabaseProps) {
    const [createMode, setCreateMode] = useState<CreateDatabaseMode>(initialMode ?? "regular");

    return (
        <Modal data-testid="create-database-modal" show size="lg" animation dialogClassName="create-database">
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
