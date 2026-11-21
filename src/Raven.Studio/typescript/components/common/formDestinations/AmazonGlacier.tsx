import { useFormContext } from "react-hook-form";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import { availableGlacierRegions } from "components/utils/common";
import { FormDestinations } from "./utils/formDestinationsTypes";
import DestinationCard from "./DestinationCard";
import AwsRegionField from "./AwsRegionField";
import { useDestinationTest } from "./useDestinationTest";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AmazonGlacier() {
    const asyncTest = useDestinationTest("glacier");

    return (
        <DestinationCard type="glacier" asyncTest={asyncTest}>
            <AmazonGlacierFields />
        </DestinationCard>
    );
}

interface AmazonGlacierFieldsProps {
    allowedRegions?: string[];
    isSessionTokenVisible?: boolean;
}

export function AmazonGlacierFields({ allowedRegions, isSessionTokenVisible }: AmazonGlacierFieldsProps) {
    const { control } = useFormContext<FormDestinations>();
    const t = useStudioTranslation("formDestinations");

    return (
        <div className="vstack gap-3">
            <FormGroup marginClass="m-0">
                <FormLabel>{t("aws.region")}</FormLabel>
                <AwsRegionField
                    name={getName("awsRegionName")}
                    availableRegions={availableGlacierRegions}
                    allowedRegions={allowedRegions}
                />
            </FormGroup>
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
                    {t("glacier.vaultName")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="formDestinations"
                                i18nKey="glacier.vaultInfo"
                                components={{ info: <span className="text-info" /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" margin="m-0" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    name={getName("vaultName")}
                    control={control}
                    placeholder={t("glacier.vaultNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            <FormGroup marginClass="m-0">
                <FormLabel>
                    {t("remoteFolderName")} <OptionalLabel />
                </FormLabel>
                <FormInput
                    name={getName("remoteFolderName")}
                    control={control}
                    placeholder={t("remoteFolderNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
        </div>
    );
}

const fieldBase = "destinations.glacier";

type FormFieldNames = keyof FormDestinations["destinations"]["glacier"];

function getName<T extends FormFieldNames>(fieldName: T): `${typeof fieldBase}.${T}` {
    return `${fieldBase}.${fieldName}`;
}
