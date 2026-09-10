import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormAceEditor, FormGroup, FormInput, FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import Code from "components/common/Code";
import Card from "react-bootstrap/Card";

export default function ImportProcessingSection() {
    const { control } = useFormContext<ImportFromFileFormData>();

    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });
    const isSetMaxReadOps = useWatch({ control, name: "processing.isSetMaxReadOpsPerSecond" });
    const isEncrypted = useWatch({ control, name: "processing.isEncrypted" });

    return (
        <ImportSection
            id="import-processing"
            title="Import processing & security"
            errorPaths={["processing.transformScript", "processing.maxReadOpsPerSecond", "processing.encryptionKey"]}
        >
            <div className="small-label mb-2">Data transformation and integrity</div>
            <div className="card p-4 mb-4">
                <FormSwitch control={control} name="processing.isUseTransformScript">
                    Use transform script{" "}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <div className="mb-1 text-center">Transform scripts are written in JavaScript</div>
                                <Code code={codeSample} language="javascript" />
                            </>
                        }
                    >
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon color="info" icon="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </FormSwitch>
                <Collapse in={isUseTransformScript}>
                    <div className="mt-3">
                        <FormAceEditor control={control} name="processing.transformScript" mode="javascript" />
                    </div>
                </Collapse>
            </div>

            <div className="small-label mb-2">Import optimization &amp; security</div>
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

const codeSample = `const name = this.FirstName;

if (name === "Bob")
    throw 'skip'; // filter-out

this.Freight = 15.3;
    `;
