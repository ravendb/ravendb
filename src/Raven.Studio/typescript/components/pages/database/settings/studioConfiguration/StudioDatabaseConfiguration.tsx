import React from "react";
import { Card, CardBody, Col, Form, InputGroup, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormSelect, FormSwitch } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    StudioDatabaseConfigurationFormData,
    studioDatabaseConfigurationYupResolver,
} from "./StudioDatabaseConfigurationValidation";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { studioEnvironmentOptions } from "components/common/studioConfiguration/StudioConfigurationUtils";
import { useServices } from "components/hooks/useServices";
import appUrl from "common/appUrl";
import { NonShardedViewProps } from "components/models/common";
import { useAccessManager } from "components/hooks/useAccessManager";

export default function StudioDatabaseConfiguration({ db }: NonShardedViewProps) {
    const { isAdminAccessOrAbove } = useAccessManager();
    const { databasesService } = useServices();

    const asyncDatabaseSettings = useAsyncCallback<StudioDatabaseConfigurationFormData>(async () => {
        const settings = await databasesService.getDatabaseStudioConfiguration(db);

        return {
            Environment: settings ? settings.Environment : "None",
            DisableAutoIndexCreation: settings ? settings.DisableAutoIndexCreation : false,
            Disabled: settings ? settings.Disabled : false,
        };
    });

    const { handleSubmit, control, formState, reset } = useForm<StudioDatabaseConfigurationFormData>({
        resolver: studioDatabaseConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncDatabaseSettings.execute,
    });

    useDirtyFlag(formState.isDirty);

    const { reportEvent } = useEventsCollector();

    const onSave: SubmitHandler<StudioDatabaseConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("studio-configuration-database", "save");
            databasesService.saveDatabaseStudioConfiguration(formData, db);
            reset(formData);
        });
    };

    const onRefresh = async () => {
        reset(await asyncDatabaseSettings.execute());
    };

    if (asyncDatabaseSettings.status === "not-requested" || asyncDatabaseSettings.status === "loading") {
        return <LoadingView />;
    }

    if (asyncDatabaseSettings.status === "error") {
        return <LoadError error="Unable to load studio configuration" refresh={onRefresh} />;
    }

    return (
        <Col lg="6" md="9" sm="12" className="gather-debug-info content-margin">
            <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                <div className="d-flex align-items-center justify-content-between">
                    <ButtonWithSpinner
                        type="submit"
                        color="primary"
                        className="mb-3"
                        icon="save"
                        disabled={!formState.isDirty || !isAdminAccessOrAbove(db)}
                        isSpinning={formState.isSubmitting}
                    >
                        Save
                    </ButtonWithSpinner>
                    <small title="Navigate to the server-wide Client Configuration View">
                        <a target="_blank" href={appUrl.forGlobalClientConfiguration()}>
                            <Icon icon="link" />
                            Go to Server-Wide Studio Configuration View
                        </a>
                    </small>
                </div>
                <Card id="popoverContainer">
                    <CardBody className="d-flex flex-center flex-column flex-wrap gap-4">
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Label className="mb-0 md-label">
                                Environment <Icon icon="info" color="info" id="environmentInfo" />
                                <UncontrolledPopover
                                    target="environmentInfo"
                                    placement="right"
                                    trigger="hover"
                                    container="popoverContainer"
                                >
                                    <PopoverBody>
                                        Change the studio environment tag. This does not affect settings or features.
                                    </PopoverBody>
                                </UncontrolledPopover>
                            </Label>
                            <FormSelect
                                control={control}
                                name="Environment"
                                options={studioEnvironmentOptions}
                            ></FormSelect>
                        </InputGroup>
                    </CardBody>
                </Card>
                <Card className="mt-3" id="disableAutoIndexesContainer">
                    <CardBody>
                        <div className="d-flex flex-column">
                            <UncontrolledPopover
                                target="disableAutoIndexesInfo"
                                placement="right"
                                trigger="hover"
                                container="disableAutoIndexesContainer"
                            >
                                <PopoverBody>
                                    <ul className="mb-0">
                                        <li>
                                            <small>
                                                Toggle on to disable creating new Auto-Indexes when making a
                                                <strong> dynamic query</strong>.
                                            </small>
                                        </li>
                                        <li>
                                            <small>
                                                Query results will be returned only when a matching Auto-Index already
                                                exists.
                                            </small>
                                        </li>
                                    </ul>
                                </PopoverBody>
                            </UncontrolledPopover>
                            <FormSwitch control={control} name="DisableAutoIndexCreation">
                                Disable creating new Auto-Indexes{" "}
                                <Icon icon="info" color="info" id="disableAutoIndexesInfo" />
                            </FormSwitch>
                        </div>
                    </CardBody>
                </Card>
            </Form>
        </Col>
    );
}
