import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import { availableS3Regions, storageClassOptions } from "components/utils/common";
import DestinationCard from "./DestinationCard";
import AwsRegionField from "./AwsRegionField";
import { useDestinationTest } from "./useDestinationTest";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AmazonS3() {
    const asyncTest = useDestinationTest("s3");

    return (
        <DestinationCard type="s3" asyncTest={asyncTest}>
            <AmazonS3Fields />
        </DestinationCard>
    );
}

interface AmazonS3FieldsProps {
    allowedRegions?: string[];
    isSessionTokenVisible?: boolean;
    isStorageClassVisible?: boolean;
}

export function AmazonS3Fields({ allowedRegions, isSessionTokenVisible, isStorageClassVisible }: AmazonS3FieldsProps) {
    const { control } = useFormContext<FormDestinations>();
    const isUseCustomHost = useWatch({ control, name: getName("isUseCustomHost") });
    const t = useStudioTranslation("formDestinations");

    return (
        <div className="vstack gap-3">
            <FormGroup marginClass="m-0">
                <FormLabel>{t("s3.provider")}</FormLabel>
                <FormSelect
                    control={control}
                    name={getName("isUseCustomHost")}
                    options={[
                        { value: false, label: t("s3.providerAws") },
                        { value: true, label: t("s3.providerCustom") },
                    ]}
                />
            </FormGroup>
            {isUseCustomHost && (
                <FormGroup marginClass="m-0">
                    <FormLabel>{t("s3.customServerUrl")}</FormLabel>
                    <FormInput
                        control={control}
                        name={getName("customServerUrl")}
                        placeholder={t("s3.customServerUrlPlaceholder")}
                        type="text"
                        autoComplete="off"
                    />
                </FormGroup>
            )}
            <FormGroup marginClass="m-0">
                <FormLabel>
                    {t("aws.region")} {isUseCustomHost && <OptionalLabel />}
                </FormLabel>
                {isUseCustomHost ? (
                    <FormInput
                        type="text"
                        control={control}
                        name={getName("awsRegionName")}
                        placeholder={t("aws.regionPlaceholder")}
                        autoComplete="off"
                    />
                ) : (
                    <AwsRegionField
                        name={getName("awsRegionName")}
                        availableRegions={availableS3Regions}
                        allowedRegions={allowedRegions}
                    />
                )}
            </FormGroup>
            {isUseCustomHost && (
                <div className="d-flex flex-column gap-1">
                    <FormSwitch control={control} name={getName("forcePathStyle")} className="w-100">
                        <span className="d-flex gap-1 align-items-center">
                            {t("s3.forcePathStyle")}
                            <PopoverWithHoverWrapper
                                message={
                                    <StudioTrans
                                        ns="formDestinations"
                                        i18nKey="s3.forcePathStyleInfo"
                                        components={{ code: <code /> }}
                                    />
                                }
                            >
                                <Icon icon="info" color="info" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormSwitch>
                    <FormSwitch control={control} name={getName("disableChecksumValidation")} className="w-100">
                        <span className="d-flex gap-1 align-items-center">
                            {t("s3.disableChecksum")}
                            <PopoverWithHoverWrapper message={t("s3.disableChecksumInfo")}>
                                <Icon icon="info" color="info" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormSwitch>
                </div>
            )}
            <FormGroup marginClass="m-0">
                <FormLabel>{t("aws.accessKey")}</FormLabel>
                <FormInput
                    name={getName("awsAccessKey")}
                    control={control}
                    placeholder={t("aws.accessKeyPlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            <FormGroup marginClass="m-0">
                <FormLabel>{t("aws.secretKey")}</FormLabel>
                <FormInput
                    name={getName("awsSecretKey")}
                    control={control}
                    placeholder={t("aws.secretKeyPlaceholder")}
                    type="password"
                    passwordPreview
                    autoComplete="off"
                />
            </FormGroup>
            {isSessionTokenVisible && (
                <FormGroup marginClass="m-0">
                    <FormLabel>
                        {t("aws.sessionToken")} <OptionalLabel />
                    </FormLabel>
                    <FormInput
                        name={getName("awsSessionToken")}
                        control={control}
                        placeholder={t("aws.sessionTokenPlaceholder")}
                        type="password"
                        passwordPreview
                        autoComplete="off"
                    />
                </FormGroup>
            )}
            <FormGroup marginClass="m-0">
                <FormLabel className="d-flex align-items-center gap-1">
                    {t("s3.bucketName")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="formDestinations"
                                i18nKey="bucketInfo"
                                components={{ info: <span className="text-info" /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" margin="m-0" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    control={control}
                    name={getName("bucketName")}
                    placeholder={t("s3.bucketNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            <FormGroup marginClass="m-0">
                <FormLabel>
                    {t("remoteFolderName")} <OptionalLabel />
                </FormLabel>
                <FormInput
                    control={control}
                    name={getName("remoteFolderName")}
                    placeholder={t("remoteFolderNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            {isStorageClassVisible && (
                <FormGroup marginClass="m-0">
                    <FormLabel>{t("s3.storageClass")}</FormLabel>
                    <FormSelect control={control} name={getName("storageClass")} options={storageClassOptions} />
                </FormGroup>
            )}
        </div>
    );
}

const fieldBase = "destinations.s3";

type FormFieldNames = keyof FormDestinations["destinations"]["s3"];

function getName<T extends FormFieldNames>(fieldName: T): `${typeof fieldBase}.${T}` {
    return `${fieldBase}.${fieldName}`;
}
