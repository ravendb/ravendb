import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { Button, Collapse, Form, FormGroup } from "reactstrap";
import { useRavenLink } from "hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import * as yup from "yup";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormCheckbox, FormInput } from "components/common/Form";
import router from "plugins/router";
import classNames from "classnames";
import genUtils from "common/generalUtils";

type FeatureImpression = "positive" | "negative";

interface FeedbackFormProps {
    goBack: () => void;
}

export function FeedbackFormContent({ goBack }: FeedbackFormProps) {
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const serverVersion = useAppSelector(clusterSelectors.serverVersion).FullVersion;
    const clientVersion = useAppSelector(clusterSelectors.clientVersion);

    const gitHubCommunityUrl = useRavenLink({ hash: "ITXUEA" });
    const cloudRequestSupportUrl = useRavenLink({ hash: "2YGOL1" });
    const onPremiseRequestSupportUrl = "https://ravendb.net/support/supportrequest?licenseId=" + licenseId;

    const requestSupportUrl = isCloud ? cloudRequestSupportUrl : onPremiseRequestSupportUrl;

    const route = genUtils.getSingleRoute(router.activeInstruction()?.config?.route);
    const moduleTitle = router.activeInstruction()?.config?.title;

    const { control, formState, handleSubmit, reset, setValue } = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            name: "",
            email: "",
            message: "",
            isFeatureSpecific: false,
            featureImpression: null,
        },
    });

    const { featureImpression, isFeatureSpecific } = useWatch({ control });

    const changeFeatureImpression = (value: FeatureImpression) => {
        if (featureImpression === value) {
            setValue("featureImpression", null);
        } else {
            setValue("featureImpression", value);
        }
    };

    const { resourcesService } = useServices();

    const handleSendFeedback: SubmitHandler<FormData> = (formData) => {
        return tryHandleSubmit(async () => {
            const dto: Raven.Server.Documents.Studio.FeedbackForm = {
                Message: formData.message,
                Product: {
                    FeatureImpression: formData.isFeatureSpecific ? formData.featureImpression : null,
                    FeatureName: formData.isFeatureSpecific ? moduleTitle : null,
                    StudioView: route,
                    StudioVersion: clientVersion,
                    Version: serverVersion,
                    Name: "RavenDB",
                },
                User: {
                    Name: formData.name,
                    Email: formData.email,
                    UserAgent: navigator.userAgent,
                },
            };

            await resourcesService.sendFeedback(dto);
            reset();
            goBack();
        });
    };

    return (
        <Form onSubmit={handleSubmit(handleSendFeedback)}>
            <ul className="action-menu__list">
                <FormGroup noMargin>
                    <FormInput placeholder="Your name" type="text" control={control} name="name" />
                </FormGroup>
                <FormGroup noMargin>
                    <FormInput placeholder="Your email" type="email" control={control} name="email" />
                </FormGroup>
                <FormGroup noMargin>
                    <FormInput placeholder="Message" type="textarea" rows={8} control={control} name="message" />
                </FormGroup>
                <FormGroup noMargin>
                    <FormCheckbox control={control} name="isFeatureSpecific">
                        <span className="fw-normal">
                            Is your feedback related to the <strong>{moduleTitle}</strong> feature?
                        </span>
                    </FormCheckbox>
                </FormGroup>
                <div className="d-flex align-items-center">
                    <Collapse isOpen={isFeatureSpecific}>
                        <div className="d-flex gap-1">
                            <Button
                                color="link"
                                className={classNames("p-0", {
                                    "text-body": featureImpression === null,
                                    "text-success": featureImpression === "positive",
                                    "text-secondary": featureImpression === "negative",
                                })}
                                onClick={() => changeFeatureImpression("positive")}
                            >
                                <Icon icon="thumb-up" margin="m-0" title="Positive" />
                            </Button>
                            <Button
                                color="link"
                                className={classNames("p-0", {
                                    "text-body": featureImpression === null,
                                    "text-danger": featureImpression === "negative",
                                    "text-secondary": featureImpression === "positive",
                                })}
                                onClick={() => changeFeatureImpression("negative")}
                            >
                                <Icon icon="thumb-down" margin="m-0" title="Negative" />
                            </Button>
                        </div>
                    </Collapse>
                    <FlexGrow />
                    <ButtonWithSpinner
                        type="submit"
                        color="primary"
                        className="rounded-pill"
                        icon="paperplane"
                        isSpinning={formState.isSubmitting}
                    >
                        Send feedback
                    </ButtonWithSpinner>
                </div>
            </ul>
            <div className="action-menu__footer">
                <small className="text-muted lh-1">
                    <Icon icon="github" />
                    Join our{" "}
                    <a href={gitHubCommunityUrl} target="_blank">
                        GitHub community
                    </a>
                </small>
                <small className="text-muted lh-1 mt-1">
                    <Icon icon="support" />
                    Need help?{" "}
                    <a href={requestSupportUrl} target="_blank">
                        Contact support
                    </a>
                </small>
            </div>
        </Form>
    );
}

const schema = yup.object({
    name: yup.string().required(),
    email: yup.string().required().email(),
    message: yup.string().required(),
    isFeatureSpecific: yup.boolean(),
    featureImpression: yup.string<FeatureImpression>().nullable(),
});

type FormData = yup.InferType<typeof schema>;
