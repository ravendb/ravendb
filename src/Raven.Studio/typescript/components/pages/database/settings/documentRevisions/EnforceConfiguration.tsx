import React from "react";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import * as yup from "yup";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import FormCollectionsSelect from "components/common/FormCollectionsSelect";
import { FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";

interface EnforceConfigurationProps {
    toggle: () => void;
    onConfirm: (includeForceCreated: boolean, collections: string[], maxOpsPerSecond: number) => Promise<void>;
}

export default function EnforceConfiguration(props: EnforceConfigurationProps) {
    const { toggle, onConfirm } = props;

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const { control, formState, setValue, handleSubmit, register } = useForm<FormData>({
        resolver: formResolver,
        defaultValues: {
            isIncludeForceCreated: false,
            isAllCollections: false,
            collections: [],
            maxOpsPerSecond: null,
        },
    });

    const { isAllCollections, collections } = useWatch({ control });

    const onEnforce: SubmitHandler<FormData> = (formData) => {
        const formCollections = formData.isAllCollections ? null : formData.collections;

        onConfirm(formData.isIncludeForceCreated, formCollections, formData.maxOpsPerSecond);
        toggle();
    };

    return (
        <Modal show onHide={toggle} contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Body>
                <Form id="enforce-configuration" className="vstack gap-2" onSubmit={handleSubmit(onEnforce)}>
                    <FormCollectionsSelect
                        control={control}
                        collectionsFormName="collections"
                        collections={collections}
                        isAllCollectionsFormName="isAllCollections"
                        isAllCollections={isAllCollections}
                        allCollectionNames={allCollectionNames}
                        setValue={setValue}
                        isCreatable={false}
                    />
                    <FormSwitch
                        control={control}
                        name="isIncludeForceCreated"
                        className="mt-2"
                        disabled={formState.isSubmitting}
                    >
                        Include Force Created Revisions
                    </FormSwitch>
                    <Form.Group className="mt-2">
                        <Form.Label>Max operations per second</Form.Label>
                        <Form.Control
                            type="number"
                            placeholder="No limit"
                            min={1}
                            {...register("maxOpsPerSecond", { valueAsNumber: true })}
                            isInvalid={!!formState.errors.maxOpsPerSecond}
                            disabled={formState.isSubmitting}
                        />
                        <Form.Control.Feedback type="invalid">
                            {formState.errors.maxOpsPerSecond?.message}
                        </Form.Control.Feedback>
                        <Form.Text className="text-muted">
                            Limits the number of revisions processed per second. Leave empty for no limit.
                        </Form.Text>
                    </Form.Group>
                    <hr />
                    <p>
                        Clicking <strong>Enforce</strong> will enforce the current revision configuration definitions{" "}
                        <strong>on all existing revisions</strong> in the database per collection.
                    </p>
                    <p>Revisions might be removed depending on the current configuration rules.</p>
                    <RichAlert variant="warning">
                        <p>For collections without a specific revision configuration:</p>
                        <ul>
                            <li>
                                <strong>Non-conflicting documents</strong>
                                <br />
                                If Document Defaults are defined & enabled, it will be applied. If not defined, or if
                                disabled, <strong>all non-conflicting document revisions will be deleted.</strong>
                            </li>
                            <li className="mt-3">
                                <strong>Conflicting documents</strong>
                                <br />
                                If Conflicting Document Defaults are enabled, it will be applied to conflicting document
                                revisions. If disabled,{" "}
                                <strong>all conflicting document revisions will be deleted.</strong>
                            </li>
                        </ul>
                    </RichAlert>
                </Form>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={toggle}>
                    Cancel
                </Button>
                <Button form="enforce-configuration" type="submit" variant="warning">
                    <Icon icon="rocket" />
                    Enforce Configuration
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

const schema = yup.object({
    isIncludeForceCreated: yup.boolean(),
    isAllCollections: yup.boolean(),
    collections: yup
        .array()
        .of(yup.string())
        .when("isAllCollections", {
            is: false,
            then: (schema) => schema.min(1),
        }),
    maxOpsPerSecond: yup
        .number()
        .nullable()
        .transform((value) => (isNaN(value) ? null : value))
        .min(1, "Max operations per second must be greater than 0"),
});

const formResolver = yupResolver(schema);
type FormData = yup.InferType<typeof schema>;
