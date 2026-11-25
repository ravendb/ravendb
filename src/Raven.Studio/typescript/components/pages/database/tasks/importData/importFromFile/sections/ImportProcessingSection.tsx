import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { FormGroup, FormInput, FormSwitch } from "components/common/Form";
import ImportSection from "components/pages/database/tasks/importData/shared/ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import TransformScriptCard from "components/pages/database/tasks/importData/shared/TransformScriptCard";
import Card from "react-bootstrap/Card";

export default function ImportProcessingSection() {
    const { control } = useFormContext<ImportFromFileFormData>();

    const isSetMaxReadOps = useWatch({ control, name: "processing.isSetMaxReadOpsPerSecond" });
    const isEncrypted = useWatch({ control, name: "processing.isEncrypted" });

    return (
        <ImportSection
            id="import-processing"
            title="Import processing & security"
            errorPaths={["processing.transformScript", "processing.maxReadOpsPerSecond", "processing.encryptionKey"]}
        >
            <TransformScriptCard />

            <h4 className="mb-2">Import optimization &amp; security</h4>
            <Card className="p-4">
                <FormGroup marginClass="mb-0">
                    <FormSwitch control={control} name="processing.isSetMaxReadOpsPerSecond">
                        Set max read operations per second
                    </FormSwitch>
                    <Collapse in={isSetMaxReadOps}>
                        <div className="mt-3">
                            <FormInput
                                control={control}
                                name="processing.maxReadOpsPerSecond"
                                type="number"
                                placeholder="Max read operations per second"
                            />
                        </div>
                    </Collapse>
                </FormGroup>
                <hr className="my-1" />
                <FormGroup marginClass="mb-0">
                    <FormSwitch control={control} name="processing.isEncrypted">
                        Imported file is encrypted
                    </FormSwitch>
                    <Collapse in={isEncrypted}>
                        <div className="mt-3">
                            <FormInput
                                control={control}
                                name="processing.encryptionKey"
                                type="password"
                                passwordPreview
                                placeholder="Key"
                                autoComplete="off"
                            />
                            <div className="small text-muted mt-1">Encryption Key (Base64 Encoding)</div>
                        </div>
                    </Collapse>
                </FormGroup>
            </Card>
        </ImportSection>
    );
}
