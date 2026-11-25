import Button from "react-bootstrap/Button";
import classNames from "classnames";
import { useFormContext } from "react-hook-form";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import useBoolean from "components/hooks/useBoolean";
import { FormDestinations } from "./utils/formDestinationsTypes";
import DestinationCard from "./DestinationCard";
import { useDestinationTest } from "./useDestinationTest";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface GoogleCloudProps {
    isForNewConnection: boolean;
}

export default function GoogleCloud({ isForNewConnection }: GoogleCloudProps) {
    const asyncTest = useDestinationTest("googleCloud");

    return (
        <DestinationCard type="googleCloud" asyncTest={asyncTest}>
            <GoogleCloudFields isCredentialsVisibleByDefault={isForNewConnection} />
        </DestinationCard>
    );
}

interface GoogleCloudFieldsProps {
    isCredentialsVisibleByDefault: boolean;
}

export function GoogleCloudFields({ isCredentialsVisibleByDefault }: GoogleCloudFieldsProps) {
    const { control, getValues } = useFormContext<FormDestinations>();
    const t = useStudioTranslation("formDestinations");

    const { value: isCredentialsJsonVisible, toggle: toggleCredentialsJsonVisible } = useBoolean(
        isCredentialsVisibleByDefault || !getValues(getName("googleCredentialsJson"))
    );

    return (
        <div className="vstack gap-3">
            <FormGroup marginClass="m-0">
                <FormLabel className="d-flex align-items-center justify-content-between">
                    {t("googleCloud.credentialsJson")}
                    <Button variant="link" size="xs" className="p-0" onClick={toggleCredentialsJsonVisible}>
                        <Icon icon={isCredentialsJsonVisible ? "preview-off" : "preview"} />
                        {isCredentialsJsonVisible ? t("googleCloud.hideCredentials") : t("googleCloud.showCredentials")}
                    </Button>
                </FormLabel>
                <FormInput
                    name={getName("googleCredentialsJson")}
                    control={control}
                    placeholder={t("googleCloud.credentialsPlaceholder")}
                    type="textarea"
                    as="textarea"
                    autoComplete="off"
                    rows={10}
                    className={classNames({ "d-none": !isCredentialsJsonVisible })}
                />
            </FormGroup>
            <FormGroup marginClass="m-0">
                <FormLabel className="d-flex align-items-center gap-1">
                    {t("googleCloud.bucket")}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <StudioTrans
                                    ns="formDestinations"
                                    i18nKey="bucketInfo"
                                    components={{ info: <span className="text-info" /> }}
                                />
                                <hr className="my-2" />
                                <a href="https://cloud.google.com/storage/docs/bucket-naming" target="_blank">
                                    <Icon icon="newtab" />
                                    {t("googleCloud.namingGuidelines")}
                                </a>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="m-0" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    name={getName("bucketName")}
                    control={control}
                    placeholder={t("googleCloud.bucketPlaceholder")}
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

const fieldBase = "destinations.googleCloud";

type FormFieldNames = keyof FormDestinations["destinations"]["googleCloud"];

function getName<T extends FormFieldNames>(fieldName: T): `${typeof fieldBase}.${T}` {
    return `${fieldBase}.${fieldName}`;
}
