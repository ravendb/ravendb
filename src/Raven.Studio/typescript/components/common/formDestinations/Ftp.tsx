import { ChangeEvent } from "react";
import InputGroup from "react-bootstrap/InputGroup";
import { useFormContext, useWatch } from "react-hook-form";
import { FormGroup, FormInput, FormLabel, FormValidationMessage } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import fileImporter from "common/fileImporter";
import { FormDestinations } from "./utils/formDestinationsTypes";
import DestinationCard from "./DestinationCard";
import { useDestinationTest } from "./useDestinationTest";
import { getFtpProtocol } from "./utils/ftpUrlUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function Ftp() {
    const asyncTest = useDestinationTest("ftp");

    return (
        <DestinationCard type="ftp" asyncTest={asyncTest}>
            <FtpFields />
        </DestinationCard>
    );
}

export function FtpFields() {
    const { control, setValue, formState } = useFormContext<FormDestinations>();
    const url = useWatch({ control, name: getName("url") });
    const certificateAsBase64 = useWatch({ control, name: getName("certificateAsBase64") });
    const t = useStudioTranslation("formDestinations");

    const isCertificateFieldVisible = getFtpProtocol(url) === "ftps";
    const certificateError = formState.errors.destinations?.ftp?.certificateAsBase64?.message;

    const selectFile = (event: ChangeEvent<HTMLInputElement>) => {
        fileImporter.readAsArrayBuffer(event.currentTarget, (data) => {
            let binary = "";
            const bytes = new Uint8Array(data);
            for (let i = 0; i < bytes.byteLength; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            setValue(getName("certificateAsBase64"), window.btoa(binary), {
                shouldValidate: true,
                shouldDirty: true,
            });
        });
    };

    return (
        <div className="vstack gap-3">
            <FormGroup marginClass="m-0">
                <FormLabel className="d-flex align-items-center gap-1">
                    {t("ftp.host")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans ns="formDestinations" i18nKey="ftp.hostInfo" components={{ code: <code /> }} />
                        }
                    >
                        <Icon icon="info" color="info" margin="m-0" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    name={getName("url")}
                    control={control}
                    placeholder={t("ftp.hostPlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            <FormGroup marginClass="m-0">
                <FormLabel>{t("ftp.username")}</FormLabel>
                <FormInput
                    name={getName("userName")}
                    control={control}
                    placeholder={t("ftp.usernamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            <FormGroup marginClass="m-0">
                <FormLabel>{t("ftp.password")}</FormLabel>
                <FormInput
                    name={getName("password")}
                    control={control}
                    placeholder={t("ftp.passwordPlaceholder")}
                    type="password"
                    autoComplete="off"
                    passwordPreview
                />
            </FormGroup>
            {isCertificateFieldVisible && (
                <div className="position-relative">
                    <FormLabel>{t("ftp.certificate")}</FormLabel>
                    <input id="ftpCertificatePicker" type="file" onChange={selectFile} className="d-none" />
                    <InputGroup>
                        <span className="static-name form-control d-flex align-items-center">
                            {certificateAsBase64 ? t("ftp.certificateProvided") : t("ftp.selectFile")}
                        </span>
                        <InputGroup.Text>
                            <label htmlFor="ftpCertificatePicker" className="cursor-pointer">
                                <Icon icon="document" />
                                <span>{t("ftp.browse")}</span>
                            </label>
                        </InputGroup.Text>
                    </InputGroup>
                    {certificateError && <FormValidationMessage>{certificateError}</FormValidationMessage>}
                </div>
            )}
        </div>
    );
}

const fieldBase = "destinations.ftp";

type FormFieldNames = keyof FormDestinations["destinations"]["ftp"];

function getName<T extends FormFieldNames>(fieldName: T): `${typeof fieldBase}.${T}` {
    return `${fieldBase}.${fieldName}`;
}
