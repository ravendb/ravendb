import React from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
} from "components/common/RichPanel";
import { Button, Collapse, Form, InputGroup, Label } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import { HStack } from "components/common/HStack";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import useConfirm from "components/common/ConfirmDialog";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { tryHandleSubmit } from "components/utils/common";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import copyToClipboard from "common/copyToClipboard";
import { todo } from "common/developmentHelper";

interface IntegrationsUserListProps {
    initialUsername: string;
    removeUser: () => void;
}

todo(
    "Feature",
    "Damian",
    "Implement Test Connection button when server-side is ready.",
    "https://issues.hibernatingrhinos.com/issue/RavenDB-22189/PostgreSQL-test-credentials"
);

export default function IntegrationsUserList(props: IntegrationsUserListProps) {
    const { initialUsername, removeUser } = props;

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { control, formState, handleSubmit, reset, setValue } = useForm<FormData>({
        resolver: formResolver,
        defaultValues: {
            username: initialUsername,
            password: "",
        },
    });
    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control });

    const isNew = !formState.defaultValues.username;

    const { databasesService } = useServices();
    const confirm = useConfirm();

    const asyncGeneratePassword = useAsyncCallback(databasesService.generateSecret, {
        onSuccess(result) {
            setValue("password", result);
        },
    });

    const asyncDeleteUser = useAsyncCallback(
        () => databasesService.deleteIntegrationsPostgreSqlCredentials(databaseName, initialUsername),
        {
            onSuccess() {
                removeUser();
            },
        }
    );

    const onDeleteUser = async () => {
        const isConfirmed = await confirm({
            title: "Delete credentials?",
            message: <span>You&apos;re deleting PostgreSQL credentials for user: {initialUsername}</span>,
            actionColor: "danger",
            icon: "trash",
            confirmText: "Delete",
        });

        if (isConfirmed) {
            await asyncDeleteUser.execute();
        }
    };

    const saveCredentials: SubmitHandler<FormData> = (formData) => {
        return tryHandleSubmit(async () => {
            databasesService.saveIntegrationsPostgreSqlCredentials(databaseName, formData.username, formData.password);
            reset(formData);
        });
    };

    return (
        <RichPanel>
            <Form onSubmit={handleSubmit(saveCredentials)}>
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            {formValues.username || "New credentials"}
                            {(formState.isDirty || isNew) && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    {hasDatabaseAdminAccess && (
                        <RichPanelActions>
                            {isNew ? (
                                <>
                                    <ButtonWithSpinner
                                        type="submit"
                                        color="success"
                                        title="Save credentials"
                                        isSpinning={formState.isSubmitting}
                                        icon="save"
                                    >
                                        Save credentials
                                    </ButtonWithSpinner>
                                    <Button
                                        type="button"
                                        color="secondary"
                                        title="Discard changes"
                                        onClick={removeUser}
                                    >
                                        <Icon icon="cancel" />
                                        Discard
                                    </Button>
                                </>
                            ) : (
                                <ButtonWithSpinner
                                    type="button"
                                    color="danger"
                                    title="Delete credentials"
                                    onClick={onDeleteUser}
                                    isSpinning={asyncDeleteUser.loading}
                                    icon="trash"
                                />
                            )}
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
                <Collapse isOpen={isNew}>
                    <RichPanelDetails className="vstack gap-3 p-4">
                        <InputGroup className="vstack mb-1">
                            <Label>Username</Label>
                            <FormInput
                                control={control}
                                name="username"
                                type="text"
                                placeholder="Enter your username"
                                autoComplete="off"
                            />
                        </InputGroup>
                        <InputGroup className="vstack">
                            <Label>Password</Label>
                            <HStack className="gap-1">
                                <div className="position-relative flex-grow">
                                    <FormInput
                                        control={control}
                                        name="password"
                                        type="password"
                                        placeholder="Enter your password"
                                        passwordPreview
                                    />
                                </div>
                                <ButtonWithSpinner
                                    type="button"
                                    title="Generate a random password"
                                    onClick={asyncGeneratePassword.execute}
                                    icon="random"
                                    isSpinning={asyncGeneratePassword.loading}
                                >
                                    Generate password
                                </ButtonWithSpinner>
                                <Button
                                    type="button"
                                    title="Copy to clipboard"
                                    onClick={() =>
                                        copyToClipboard.copy(formValues.password, "Password was copied to clipboard.")
                                    }
                                >
                                    <Icon icon="copy-to-clipboard" margin="m-0" />
                                </Button>
                            </HStack>
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </Form>
        </RichPanel>
    );
}

const schema = yup.object({
    username: yup.string().required(),
    password: yup.string().required(),
});

const formResolver = yupResolver(schema);
type FormData = yup.InferType<typeof schema>;
