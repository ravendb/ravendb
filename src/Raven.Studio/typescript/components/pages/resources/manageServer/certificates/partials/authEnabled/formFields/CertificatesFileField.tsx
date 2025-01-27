import fileImporter from "common/fileImporter";
import { FormValidationMessage } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CertificatesUploadFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesUploadModal";
import { useState, ChangeEvent, ReactNode } from "react";
import { useFormContext } from "react-hook-form";
import { FormGroup, InputGroup, InputGroupText, Label, PopoverBody, UncontrolledPopover } from "reactstrap";

export default function CertificatesFileField({ infoPopoverBody }: { infoPopoverBody: ReactNode }) {
    const { formState, setValue } = useFormContext<CertificatesUploadFormData>();

    const [importedFileName, setImportedFileName] = useState<string>(null);

    const selectFile = (event: ChangeEvent<HTMLInputElement>) => {
        fileImporter.readAsDataURL(event.currentTarget, (dataUrl, fileName) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;
            setImportedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);

            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            setValue("certificateAsBase64", dataUrl.substr(dataUrl.indexOf(",") + 1));
        });
    };

    return (
        <FormGroup>
            <Label>
                Certificate File <Icon icon="info" color="info" id="certificateFilePopover" />
            </Label>
            <UncontrolledPopover trigger="hover" placement="top" target="certificateFilePopover">
                <PopoverBody>{infoPopoverBody}</PopoverBody>
            </UncontrolledPopover>
            <input id="filePicker" type="file" onChange={selectFile} className="d-none" />
            <InputGroup>
                <span className="static-name form-control d-flex align-items-center">
                    {importedFileName ? importedFileName : "Select file..."}
                </span>
                <InputGroupText>
                    <label htmlFor="filePicker" className="cursor-pointer">
                        <Icon icon="document" />
                        <span>Browse</span>
                    </label>
                </InputGroupText>
            </InputGroup>
            {formState.errors.certificateAsBase64 && (
                <FormValidationMessage>{formState.errors.certificateAsBase64.message}</FormValidationMessage>
            )}
        </FormGroup>
    );
}
