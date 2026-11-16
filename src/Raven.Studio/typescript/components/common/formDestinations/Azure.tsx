import { useFormContext, useWatch } from "react-hook-form";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import { FormDestinations } from "./utils/formDestinationsTypes";
import AzureAuthTypeToggle from "./AzureAuthTypeToggle";
import DestinationCard from "./DestinationCard";
import { useDestinationTest } from "./useDestinationTest";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function Azure() {
    const asyncTest = useDestinationTest("azure");

    return (
        <DestinationCard type="azure" asyncTest={asyncTest}>
            <AzureFields />
        </DestinationCard>
    );
}

export function AzureFields() {
    const { control } = useFormContext<FormDestinations>();
    const authType = useWatch({ control, name: getName("authType") });
    const t = useStudioTranslation("formDestinations");

    return (
        <div className="vstack gap-3">
            <FormGroup marginClass="m-0">
                <FormLabel>{t("azure.accountName")}</FormLabel>
                <FormInput
                    name={getName("accountName")}
                    control={control}
                    placeholder={t("azure.accountNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            <div>
                <AzureAuthTypeToggle control={control} name={getName("authType")} className="mb-2" />
                {authType === "sasToken" ? (
                    <FormInput
                        key="sasToken"
                        name={getName("sasToken")}
                        control={control}
                        placeholder={t("azure.sasTokenPlaceholder")}
                        type="password"
                        passwordPreview
                        autoComplete="off"
                    />
                ) : (
                    <FormInput
                        key="accountKey"
                        name={getName("accountKey")}
                        control={control}
                        placeholder={t("azure.accountKeyPlaceholder")}
                        type="password"
                        passwordPreview
                        autoComplete="off"
                    />
                )}
            </div>
            <FormGroup marginClass="m-0">
                <FormLabel className="d-flex gap-1 align-items-center">
                    {t("azure.storageContainer")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="formDestinations"
                                i18nKey="azure.containerInfo"
                                components={{ info: <span className="text-info" /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" margin="m-0" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    name={getName("storageContainer")}
                    control={control}
                    placeholder={t("azure.storageContainerPlaceholder")}
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

const fieldBase = "destinations.azure";

type FormFieldNames = keyof FormDestinations["destinations"]["azure"];

function getName<T extends FormFieldNames>(fieldName: T): `${typeof fieldBase}.${T}` {
    return `${fieldBase}.${fieldName}`;
}
