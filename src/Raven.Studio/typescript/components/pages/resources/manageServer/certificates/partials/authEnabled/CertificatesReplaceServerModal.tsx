import { yupResolver } from "@hookform/resolvers/yup";
import { FormInput, FormSwitch, FormGroup, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import * as yup from "yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import CertificatesFileField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesFileField";
import RichAlert from "components/common/RichAlert";
import { useAsync } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import classNames from "classnames";
import Modal from "components/common/Modal";

export default function CertificatesReplaceServerModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            certificateAsBase64: "",
            certificatePassphrase: "",
            isReplaceImmediately: false,
        },
    });

    const { control, formState, handleSubmit, reset } = form;

    const formValues = useWatch({ control });

    const asyncGetClusterDomains = useAsync(manageServerService.getClusterDomains, []);

    const handleReplace: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "replace");
            await manageServerService.replaceClusterCertificate(
                certificatesUtils.mapReplaceServerToDto(formData),
                formData.isReplaceImmediately
            );
            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.isReplaceServerModalOpenToggled());
        });
    };

    return (
        <Modal show size="lg" centered contentClassName="modal-border bulge-warning">
            <FormProvider {...form}>
                <form onSubmit={handleSubmit(handleReplace)}>
                    <Modal.Header
                        className="vstack gap-4"
                        onCloseClick={() => dispatch(certificatesActions.isReplaceServerModalOpenToggled())}
                    >
                        <div className="text-center">
                            <Icon icon="refresh" className="fs-1" color="warning" margin="m-0" />
                        </div>
                        <div className="text-center lead">Replace server certificates (cluster-wide)</div>
                    </Modal.Header>
                    <Modal.Body>
                        <LazyLoad active={asyncGetClusterDomains.loading}>
                            <RichAlert title="" variant="info" className="my-2">
                                <span>
                                    Replace all server certificates in the cluster without shutting down the servers.
                                    The update will happen when all nodes of the cluster confirm the replacement, or
                                    when there is 3 days or less left until expiration.
                                </span>
                                <div className={classNames(asyncGetClusterDomains.result?.length ? "" : "d-none")}>
                                    Please verify that the new certificate contains all of the following domain names in
                                    the Subject Alternative Names field (SAN) of the certificate:
                                    <ul className="mt-2">
                                        {asyncGetClusterDomains.result?.map((domain, idx) => (
                                            <li key={idx}>{domain}</li>
                                        ))}
                                    </ul>
                                </div>
                            </RichAlert>
                        </LazyLoad>
                        <CertificatesFileField infoPopoverBody="Certificate file cannot be password protected." />
                        <FormGroup>
                            <FormLabel>Certificate Passphrase</FormLabel>
                            <FormInput type="password" control={control} name="certificatePassphrase" passwordPreview />
                        </FormGroup>
                        <FormGroup>
                            <FormSwitch control={control} name="isReplaceImmediately">
                                Replace immediately
                            </FormSwitch>
                        </FormGroup>
                        {formValues.isReplaceImmediately && (
                            <RichAlert variant="warning">
                                If Replace immediately is specified, RavenDB will replace certificate by force, even if
                                some nodes are not responding. In that case, you will have to manually replace the
                                certificate in those nodes. <strong>Use with care.</strong>
                            </RichAlert>
                        )}
                    </Modal.Body>
                    <Modal.Footer>
                        <Button
                            variant="link"
                            onClick={() => dispatch(certificatesActions.isReplaceServerModalOpenToggled())}
                            className="link-muted"
                        >
                            Cancel
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            variant="warning"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                        >
                            Replace certificate
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    certificateAsBase64: yup.string().required(),
    certificatePassphrase: yup.string(),
    isReplaceImmediately: yup.boolean(),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesReplaceServerFormData = FormData;
