import Modal from "components/common/Modal";
import { Controller, ControllerRenderProps, FieldValues, FormProvider, useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { Icon } from "components/common/Icon";
import { yupResolver } from "@hookform/resolvers/yup";
import { FormDatePicker, FormGroup, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import FileUploadPanel from "components/common/FileUploadPanel";
import { FileUploadItem } from "components/common/FileUploadList";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import React, { useState } from "react";
import document from "models/database/documents/document";
import database from "models/resources/database";
import Button from "react-bootstrap/Button";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "hooks/useServices";
import moment from "moment";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import RichAlert from "components/common/RichAlert";
import { components, GroupBase, OptionProps } from "react-select";
import editDocumentUploader = require("viewmodels/database/documents/editDocumentUploader");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import RemoteAttachmentParameters = Raven.Client.Documents.Operations.Attachments.RemoteAttachmentParameters;

type AddAttachmentWithRemoteParametersModalProps = {
    document: KnockoutObservable<document>;
    db: database;
    onUploaded: () => void;
    onClose: () => void;
};

type FileUploadState = Omit<FileUploadItem, "file">;

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
    const [uploader] = useState(() => new editDocumentUploader(document, db, onUploaded));
    const [batchProgress, setBatchProgress] = useState<attachmentUploadProgress>(null);
    const [uploadStates, setUploadStates] = useState<Record<string, FileUploadState>>({});

    const selectedDestination = useWatch({
        control,
        name: "identifier",
    });

    const selectedFiles = useWatch({ control, name: "files" }) as File[];
    const selectedUploadDate = useWatch({ control, name: "uploadDate" }) as Date;

    const onProgress = (progress: attachmentUploadProgress) => {
        setBatchProgress(progress);
        setUploadStates((states) => ({
            ...states,
            [progress.fileName]: { status: progress.status, loaded: progress.loaded, total: progress.total },
        }));
    };

    const handleSubmit = async (formData: AttachmentWithRemoteParametersFormData) => {
        try {
            await uploader.uploadFiles(formData.files as File[], mapToDto(formData), onProgress);
        } finally {
            setBatchProgress(null);
        }
        onClose();
    };

    const onFileDropzoneChange = (files: File[], field: ControllerRenderProps<FieldValues, "files">) => {
        if (files.some((file) => !file.name.trim())) {
            messagePublisher.reportError("Failed to load file");
            form.setError("files", { type: "manual", message: "Failed to load file" });
            return;
        }

        const current: File[] = field.value ?? [];
        const replaced = current.filter((x) => !files.some((file) => file.name === x.name));
        field.onChange([...replaced, ...files]);
    };

    const removeFile = (file: File) => {
        form.setValue(
            "files",
            selectedFiles.filter((x) => x !== file),
            { shouldValidate: true, shouldDirty: true }
        );
    };

    const clearFiles = () => {
        form.setValue("files", [], { shouldValidate: true, shouldDirty: true });
    };

    const uploadItems: FileUploadItem[] = (selectedFiles ?? []).map((file) => ({
        file,
        status: "selected",
        ...uploadStates[file.name],
    }));

    const attachmentsLabel = pluralizeHelpers.pluralize(selectedFiles?.length || 1, "attachment", "attachments", true);

    return (
        <Modal size="lg" show contentClassName="modal-border bulge-info">
            <Modal.Header className="pb-0" onCloseClick={onClose}>
                <h3>
                    <Icon icon="remote-attachment" color="info" />
                    Add {attachmentsLabel} to remote storage
                </h3>
            </Modal.Header>
            <FormProvider {...form}>
                <Modal.Body>
                    <form onSubmit={form.handleSubmit(handleSubmit)}>
                        <FormGroup>
                            <Controller
                                name="files"
                                render={({ field }) => (
                                    <FileUploadPanel
                                        items={uploadItems}
                                        onChange={(files) => onFileDropzoneChange(files, field)}
                                        onRemove={removeFile}
                                        onCancel={() => uploader.abortCurrent()}
                                        onClearAll={clearFiles}
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
                                components={{ Option: RemoteAttachmentDestinationOption }}
                            />
                            <RemoteAttachmentWarning
                                config={asyncGetRemoteAttachmentParametersConfig.result}
                                selectedDestination={selectedDestination}
                            />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>Scheduled upload time</FormLabel>
                            <FormDatePicker
                                placeholderText="e.g. 11/21/2025 10:57 AM"
                                showTimeSelect
                                minDate={new Date()}
                                minTime={getMinUploadTime(selectedUploadDate)}
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
                        {batchProgress
                            ? `Uploading ${batchProgress.position}/${batchProgress.count}`
                            : `Save ${attachmentsLabel} with remote settings`}
                    </ButtonWithSpinner>
                </Modal.Footer>
            </FormProvider>
        </Modal>
    );
}

interface RemoteAttachmentWarningProps {
    config: RemoteAttachmentsStudioConfiguration;
    selectedDestination: string;
}

function RemoteAttachmentWarning({ config, selectedDestination }: RemoteAttachmentWarningProps) {
    if (config?.Disabled) {
        return (
            <RichAlert className="mt-2" icon="warning" variant="warning">
                Remote attachments feature is currently <b>disabled</b>. You can add an attachment to the remote
                storage, but it will be uploaded only after the feature is enabled.
            </RichAlert>
        );
    }

    if (config?.Destinations[selectedDestination]?.Disabled) {
        return (
            <RichAlert className="mt-2" icon="warning" variant="warning">
                Destination is currently <b>disabled</b>. You can add an attachment to the remote storage, but it will
                be uploaded only after the destination is enabled.
            </RichAlert>
        );
    }

    return null;
}

function getMinUploadTime(selectedDate: Date): Date {
    const isToday = !selectedDate || moment(selectedDate).isSame(moment(), "day");
    return isToday ? moment().subtract(29, "minutes").toDate() : moment().startOf("day").toDate();
}

const schema = yup.object({
    files: yup.array().of(yup.mixed()).min(1, "File is required").required("File is required"),
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

const getRemoteAttachmentsDestinationsOptions = (
    remoteAttachmentsConfiguration?: RemoteAttachmentsStudioConfiguration
) => {
    if (!remoteAttachmentsConfiguration) {
        return [];
    }
    return Object.keys(remoteAttachmentsConfiguration.Destinations).map((destination) => {
        const destinationKey = remoteAttachmentsConfiguration.Destinations[destination];
        return { label: destination, value: destination, disabled: destinationKey.Disabled };
    });
};

function getDefaultValues(configResult: RemoteAttachmentsStudioConfiguration): AttachmentWithRemoteParametersFormData {
    const options = getRemoteAttachmentsDestinationsOptions(configResult);
    // if there is only one destination, use it as default
    if (options.length === 1) {
        return {
            identifier: options[0].value,
            uploadDate: new Date(),
            files: [],
        };
    }

    return {
        identifier: null,
        uploadDate: new Date(),
        files: [],
    };
}

interface RemoteAttachmentDestination {
    value: string;
    label: string;
    disabled?: boolean;
}

type RemoteAttachmentDestinationOptionProps = OptionProps<
    RemoteAttachmentDestination,
    false,
    GroupBase<RemoteAttachmentDestination>
>;

function RemoteAttachmentDestinationOption(props: RemoteAttachmentDestinationOptionProps) {
    const { data, label } = props;
    return (
        <components.Option {...props}>
            {label}
            {data.disabled && (
                <PopoverWithHoverWrapper message="Destination is disabled, so it will not be uploaded to the server. Destination must be enabled to do it.">
                    <Icon icon="warning" color="warning" margin="ms-1" />
                </PopoverWithHoverWrapper>
            )}
        </components.Option>
    );
}
