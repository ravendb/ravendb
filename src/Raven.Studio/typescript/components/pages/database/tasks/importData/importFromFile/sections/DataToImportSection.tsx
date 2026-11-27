import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import ImportSection from "components/pages/database/tasks/importData/shared/ImportSection";
import DocumentsToImportCard from "components/pages/database/tasks/importData/shared/DocumentsToImportCard";
import { getItemsToWarnAbout } from "components/pages/database/tasks/importData/shared/importOptionsUtils";
import { ImportFromFileFormData } from "../importFromFileValidation";
import CollectionsToImportPicker from "./CollectionsToImportPicker";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function DataToImportSection() {
    const t = useStudioTranslation("importData");
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const documents = useWatch({ control, name: "documents" });

    const isImportAll = useWatch({ control, name: "collections.isImportAllCollections" });

    const itemsToWarnAbout = getItemsToWarnAbout({ documents });

    return (
        <ImportSection id="data-to-import" title={t("sections.dataToImport")}>
            <h4 id="collections-to-import" className="mb-2">
                {t("importFile.collections.title")}
            </h4>
            <Card className="mb-4 p-4">
                <div className="d-flex gap-2">
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: isImportAll })}
                        onClick={() => setValue("collections.isImportAllCollections", true, { shouldDirty: true })}
                    >
                        <Icon icon="documents" margin="m-0" />
                        {t("importFile.collections.importAll")}
                    </button>
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: !isImportAll })}
                        onClick={() => setValue("collections.isImportAllCollections", false, { shouldDirty: true })}
                    >
                        <Icon icon="document-group" addon="edit" margin="m-0" />
                        {t("importFile.collections.customize")}
                    </button>
                </div>
                {!isImportAll && <CollectionsToImportPicker />}
            </Card>

            <DocumentsToImportCard />
            {itemsToWarnAbout.length > 0 && (
                <Alert variant="warning" className="mt-3 mb-0">
                    <Icon icon="warning" color="warning" />{" "}
                    {t("importFile.itemsWithoutDocuments", { items: itemsToWarnAbout.join(", ") })}
                </Alert>
            )}
        </ImportSection>
    );
}
