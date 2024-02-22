import React, { useState } from "react";
import { Modal } from "reactstrap";
import CreateDatabaseRegular from "./regular/CreateDatabaseRegular";
import CreateDatabaseFromBackup from "./formBackup/CreateDatabaseFromBackup";
import "./CreateDatabase.scss";

type CreateMode = "regular" | "fromBackup";

interface CreateDatabaseProps {
    closeModal: () => void;
}

export default function CreateDatabase({ closeModal }: CreateDatabaseProps) {
    const [createMode, setCreateMode] = useState<CreateMode>("regular");

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
