import Modal from "components/common/Modal";
import { Controller, ControllerRenderProps, FieldValues, FormProvider, useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { Icon } from "components/common/Icon";
import { yupResolver } from "@hookform/resolvers/yup";
import { FormDatePicker, FormGroup, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import FileDropzone from "components/common/FileDropzone";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import * as React from "react";
import document from "models/database/documents/document";
import database from "models/resources/database";
import Button from "react-bootstrap/Button";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "hooks/useServices";
import moment from "moment";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import editDocumentUploader = require("viewmodels/database/documents/editDocumentUploader");
import RemoteAttachmentParameters = Raven.Client.Documents.Operations.Attachments.RemoteAttachmentParameters;
import RichAlert from "components/common/RichAlert";

type AddAttachmentWithRemoteParametersModalProps = {
    document: KnockoutObservable<document>;
    db: database;
    onUploaded: () => void;
    onClose: () => void;
};

export default function AddAttachmentWithRemoteParametersModal({
    document,
    onClose,
    db,
    onUploaded,
}: AddAttachmentWithRemoteParametersModalProps) {
    const { databasesService } = useServices();
    const asyncGetRemoteAttachmentParametersConfig = useAsyncCallback(() =>
        databasesService.getRemoteAttachmentsDestinations(db.name)
    );

    const form = useForm({
        resolver: yupResolver(schema),
        defaultValues: async () => {
            const config = await asyncGetRemoteAttachmentParametersConfig.execute();
            return getDefaultValues(config);
        },
    });

    const { control, formState } = form;

    const uploader = new editDocumentUploader(document, db, onUploaded);

    const asyncUploadFileWithRemoteParameters = useAsyncCallback((file: File, dto: RemoteAttachmentParameters) =>
        uploader.uploadFileWithRemoteParameters(file, dto)
    );

    const selectedDestination = useWatch({
        control, name: "identifier"
    })

    const handleSubmit = async (formData: AttachmentWithRemoteParametersFormData) => {
        try {
            await asyncUploadFileWithRemoteParameters.execute(formData.file as File, mapToDto(formData));
            onClose();
        } catch (e) {
            if ((e as Error).message === editDocumentUploader.userCancelledErrorCode) {
                return;
            }
        }
    };

    const onFileDropzoneChange = async (files: File[], field: ControllerRenderProps<FieldValues, "file">) => {
        const file = files[0];

        if (!file.name.trim()) {
            messagePublisher.reportError("Failed to load file");
            form.setError("file", { type: "manual", message: "Failed to load file" });
            return;
        }

        field.onChange(file);
    };

    return (
        <Modal size="lg" show contentClassName="modal-border bulge-info">
            <Modal.Header onCloseClick={onClose}>
                <h3>
                    <Icon icon="remote-attachment" color="info" />
                    Add attachment to remote storage
                </h3>
            </Modal.Header>
            <FormProvider {...form}>
                <Modal.Body>
                    <form onSubmit={form.handleSubmit(handleSubmit)}>
                        <FormGroup>
                            <Controller
                                name="file"
                                render={({ field }) => (
                                    <FileDropzone
                                        {...field}
                                        maxFiles={1}
                                        onChange={(files) => onFileDropzoneChange(files, field)}
                                    />
                                )}
                            ></Controller>
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>Remote destination identifier</FormLabel>
                            <FormSelectAutocomplete
                                placeholder="Select or enter a defined destination identifier"
                                isLoading={asyncGetRemoteAttachmentParametersConfig.loading}
                                options={getRemoteAttachmentsDestinationsOptions(
                                    asyncGetRemoteAttachmentParametersConfig.result
                                )}
                                name="identifier"
                                control={control}
                            />
                            {asyncGetRemoteAttachmentParametersConfig.result?.Destinations[selectedDestination]?.Disabled && (
                                <RichAlert className="mt-2" icon="warning" variant="warning">
                                    Destination is currently <b>disabled</b>. You can add an attachment to the remote storage, but it will be uploaded only after the destination is enabled.
                                </RichAlert>
                            )}
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>Scheduled upload time</FormLabel>
                            <FormDatePicker
                                placeholderText="e.g. 11/21/2025 10:57 AM"
                                showTimeSelect
                                minDate={new Date()}
                                minTime={moment().subtract(29, "minutes").toDate()}
                                maxTime={moment().endOf("day").toDate()}
                                name="uploadDate"
                                control={control}
                            />
                        </FormGroup>
                    </form>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="link" className="text-muted" onClick={onClose}>
                        Close
                    </Button>
                    <ButtonWithSpinner
                        className="rounded-pill"
                        variant="info"
                        isSpinning={formState.isSubmitting}
                        onClick={form.handleSubmit(handleSubmit)}
                        disabled={!formState.isValid}
                    >
                        Save attachment with remote settings
                    </ButtonWithSpinner>
                </Modal.Footer>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    file: yup.mixed().required("File is required"),
    identifier: yup.string().required("Identifier is required"),
    uploadDate: yup.date().required("Upload date is required"),
});

type AttachmentWithRemoteParametersFormData = yup.InferType<typeof schema>;

const mapToDto = (values: AttachmentWithRemoteParametersFormData): RemoteAttachmentParameters => {
    return {
        At: values.uploadDate.toISOString(),
        Identifier: values.identifier,
    };
};

const getRemoteAttachmentsDestinationsOptions = (remoteAttachmentsConfiguration?: RemoteAttachmentsStudioConfiguration) => {
    if (!remoteAttachmentsConfiguration) {
        return [];
    }
    return Object.keys(remoteAttachmentsConfiguration.Destinations).map((destination) => {
        const destinationKey = remoteAttachmentsConfiguration.Destinations[destination];
        const label = (
            <span>
                {destination}
                {destinationKey.Disabled && (
                        <PopoverWithHoverWrapper message="Destination is disabled, so it will not be uploaded to the server. Destination must be enabled to do it.">
                            <Icon icon="warning" color="warning" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                )}
            </span>
        );
        return { label, value: destination };
    });
};

function getDefaultValues(configResult: RemoteAttachmentsStudioConfiguration): AttachmentWithRemoteParametersFormData {
    const options = getRemoteAttachmentsDestinationsOptions(configResult);
    // if there is only one destination, use it as default
    if (options.length === 1) {
        return {
            identifier: options[0].value,
            uploadDate: new Date(),
            file: null,
        };
    }

    return {
        identifier: null,
        uploadDate: new Date(),
        file: null,
    };
}
