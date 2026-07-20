import React, { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import InputGroup from "react-bootstrap/InputGroup";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormAceEditor, FormGroup, FormInput, FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { buildImportCurlCommand, ImportCommandType } from "../importFromFileUtils";
import { useImportLicenseRestrictions } from "../useImportLicenseRestrictions";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import Code from "components/common/Code";
import copyToClipboard = require("common/copyToClipboard");

export default function ImportProcessingSection() {
    const { control } = useFormContext<ImportFromFileFormData>();
    const formData = useWatch({ control });
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const [commandType, setCommandType] = useState<ImportCommandType>("PowerShell");

    const isUseTransformScript = !!formData.processing?.isUseTransformScript;
    const isSetMaxReadOps = !!formData.processing?.isSetMaxReadOpsPerSecond;
    const isEncrypted = !!formData.processing?.isEncrypted;

    const { restrictedFeatures, restrictedOngoingTasks } = useImportLicenseRestrictions();
    const curlCommand = buildImportCurlCommand(
        commandType,
        formData as ImportFromFileFormData,
        databaseName,
        restrictedFeatures.map((x) => x.settingKey),
        restrictedOngoingTasks.map((x) => x.taskKey)
    );

    return (
        <ImportSection id="import-processing" title="Import processing & security">
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
            <div className="card p-4 mb-4">
                <FormGroup>
                    <FormSwitch control={control} name="processing.isSetMaxReadOpsPerSecond">
                        Set max read operations per second
                    </FormSwitch>
                    <Collapse in={isSetMaxReadOps}>
                        <div>
                            <FormInput
                                control={control}
                                name="processing.maxReadOpsPerSecond"
                                type="number"
                                placeholder="Max read operations per second"
                            />
                        </div>
                    </Collapse>
                </FormGroup>
                <FormGroup>
                    <FormSwitch control={control} name="processing.isEncrypted">
                        Imported file is encrypted
                    </FormSwitch>
                    <Collapse in={isEncrypted}>
                        <div>
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
            </div>

            <div className="small-label mb-2">Import command</div>
            <div className="card p-4">
                <InputGroup>
                    <Dropdown>
                        <Dropdown.Toggle variant="secondary">Import Command - {commandType}</Dropdown.Toggle>
                        <Dropdown.Menu>
                            {commandTypes.map((type) => (
                                <Dropdown.Item key={type} onClick={() => setCommandType(type)}>
                                    Import Command - {type}
                                </Dropdown.Item>
                            ))}
                        </Dropdown.Menu>
                    </Dropdown>
                    <Form.Control
                        readOnly
                        value={curlCommand}
                        onClick={(e) => (e.target as HTMLInputElement).select()}
                    />
                    <Button
                        variant="secondary"
                        title="Copy import command"
                        onClick={() => copyToClipboard.copy(curlCommand, "Import command was copied to clipboard.")}
                    >
                        <Icon icon="copy-to-clipboard" margin="m-0" />
                    </Button>
                </InputGroup>
            </div>
        </ImportSection>
    );
}

const codeSample = `const name = this.FirstName;
    
if (name === "Bob")
    throw 'skip'; // filter-out
    
this.Freight = 15.3;
    `;

const commandTypes: ImportCommandType[] = ["PowerShell", "Cmd", "Bash"];
