import React from "react";
import { Button, Form, Modal, ModalBody, ModalFooter } from "reactstrap";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import * as yup from "yup";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import FormCollectionsSelect from "components/common/FormCollectionsSelect";
import { FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";

interface EnforceConfigurationProps {
    toggle: () => void;
    onConfirm: (includeForceCreated: boolean, collections: string[]) => Promise<void>;
}

export default function EnforceConfiguration(props: EnforceConfigurationProps) {
    const { toggle, onConfirm } = props;

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const { control, formState, setValue, handleSubmit } = useForm<FormData>({
        resolver: formResolver,
        defaultValues: {
            isIncludeForceCreated: false,
            isAllCollections: false,
            collections: [],
        },
    });

    const { isAllCollections, collections } = useWatch({ control });

    const onEnforce: SubmitHandler<FormData> = (formData) => {
        const formCollections = formData.isAllCollections ? null : formData.collections;

        onConfirm(formData.isIncludeForceCreated, formCollections);
        toggle();
    };

    return (
        <Modal
            isOpen
            toggle={toggle}
            wrapClassName="bs5"
            contentClassName="modal-border bulge-warning"
            centered
            size="lg"
        >
            <Form onSubmit={handleSubmit(onEnforce)}>
                <ModalBody className="vstack gap-2">
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
                </ModalBody>
                <ModalFooter>
                    <Button color="link" className="link-muted" onClick={toggle}>
                        Cancel
                    </Button>
                    <Button type="submit" color="warning">
                        <Icon icon="rocket" />
                        Enforce Configuration
                    </Button>
                </ModalFooter>
            </Form>
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
});

const formResolver = yupResolver(schema);
type FormData = yup.InferType<typeof schema>;
