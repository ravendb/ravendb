import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { FormGroup, FormInput, FormSwitch } from "components/common/Form";
import ImportSection from "components/pages/database/tasks/importData/shared/ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import TransformScriptCard from "components/pages/database/tasks/importData/shared/TransformScriptCard";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import Card from "react-bootstrap/Card";

export default function ImportProcessingSection() {
    const t = useStudioTranslation("importData");
    const { control } = useFormContext<ImportFromFileFormData>();

    const isSetMaxReadOps = useWatch({ control, name: "processing.isSetMaxReadOpsPerSecond" });
    const isEncrypted = useWatch({ control, name: "processing.isEncrypted" });

    return (
        <ImportSection
            id="import-processing"
            title={t("sections.importProcessingAndSecurity")}
            errorPaths={["processing.transformScript", "processing.maxReadOpsPerSecond", "processing.encryptionKey"]}
        >
            <TransformScriptCard />

            <h4 className="mb-2">{t("importFile.optimizationAndSecurity")}</h4>
            <Card className="p-4">
                <FormGroup marginClass="mb-0">
                    <FormSwitch control={control} name="processing.isSetMaxReadOpsPerSecond">
                        {t("importFile.setMaxReadOps")}
                    </FormSwitch>
                    <Collapse in={isSetMaxReadOps}>
                        <div className="mt-3">
                            <FormInput
                                control={control}
                                name="processing.maxReadOpsPerSecond"
                                type="number"
                                placeholder={t("importFile.maxReadOpsPlaceholder")}
                            />
                        </div>
                    </Collapse>
                </FormGroup>
                <hr className="my-1" />
                <FormGroup marginClass="mb-0">
                    <FormSwitch control={control} name="processing.isEncrypted">
                        {t("importFile.isEncrypted")}
                    </FormSwitch>
                    <Collapse in={isEncrypted}>
                        <div className="mt-3">
                            <FormInput
                                control={control}
                                name="processing.encryptionKey"
                                type="password"
                                passwordPreview
                                placeholder={t("importFile.encryptionKeyPlaceholder")}
                                autoComplete="off"
                            />
                            <div className="small text-muted mt-1">{t("importFile.encryptionKeyHint")}</div>
                        </div>
                    </Collapse>
                </FormGroup>
            </Card>
        </ImportSection>
    );
}
