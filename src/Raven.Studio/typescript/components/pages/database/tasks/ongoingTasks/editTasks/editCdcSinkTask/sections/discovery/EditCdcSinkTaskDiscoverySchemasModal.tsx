import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import InnerForm from "components/common/InnerForm";
import Modal from "components/common/Modal";
import RichAlert from "components/common/RichAlert";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import { SubmitHandler, useForm } from "react-hook-form";
import Button from "react-bootstrap/Button";
import { UseAsyncReturn } from "react-async-hook";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

interface EditCdcSinkTaskDiscoverySchemasModalProps {
    asyncGetSchema: UseAsyncReturn<
        Raven.Client.Documents.Operations.CdcSink.Schema.CdcSinkSourceSchema,
        [schemas: string[]]
    >;
    onClose: () => void;
}

export default function EditCdcSinkTaskDiscoverySchemasModal({
    asyncGetSchema,
    onClose,
}: EditCdcSinkTaskDiscoverySchemasModalProps) {
    const { control, handleSubmit } = useForm<DiscoverySchemasFormData>({
        defaultValues: {
            schemas: [],
        },
        resolver: yupResolver(schema),
    });

    const handleDiscover: SubmitHandler<DiscoverySchemasFormData> = async (formData) => {
        const schemas = formData.schemas.map((x) => x.value);
        await asyncGetSchema.execute(schemas.length > 0 ? schemas : null);
        onClose();
    };

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-primary">
            <Modal.Header onCloseClick={onClose} className="pb-0">
                <h3 className="m-0">Discover tables</h3>
            </Modal.Header>
            <InnerForm onSubmit={handleSubmit(handleDiscover)}>
                <Modal.Body className="vstack gap-3">
                    <RichAlert variant="info">
                        Leave schemas empty to discover tables from the default schemas for the selected connection.
                    </RichAlert>
                    <FormStringValueList
                        title="Schemas"
                        addButtonLabel="Add schema"
                        control={control}
                        name="schemas"
                        fieldNameAccessor={(idx) => `schemas.${idx}.value`}
                        defaultValue={{ value: "" }}
                    />
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="link" onClick={onClose}>
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        variant="primary"
                        onClick={handleSubmit(handleDiscover)}
                        isSpinning={asyncGetSchema.loading}
                        className="rounded-pill"
                        icon="search"
                    >
                        Discover
                    </ButtonWithSpinner>
                </Modal.Footer>
            </InnerForm>
        </Modal>
    );
}

const schema = yup.object({
    schemas: yup.array().of(
        yup.object({
            value: yup.string().trim().strict().required(),
        })
    ),
});

export type DiscoverySchemasFormData = yup.InferType<typeof schema>;
