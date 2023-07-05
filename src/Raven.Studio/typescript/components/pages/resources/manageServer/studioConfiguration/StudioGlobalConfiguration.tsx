import React from "react";
import { Card, CardBody, Col, Form, InputGroup, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    StudioGlobalConfigurationFormData,
    allStudioEnvironments,
    studioGlobalConfigurationYupResolver,
} from "./StudioGlobalConfigurationValidation";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
import { SelectOption } from "components/common/Select";
import studioSettings = require("common/settings/studioSettings");
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { DevTool } from "@hookform/devtools";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";

export default function StudioGlobalConfiguration() {
    const asyncGlobalSettings = useAsyncCallback(async () => {
        const settings = await studioSettings.default.globalSettings(true);

        return {
            environment: settings.environment.getValue(),
            replicationFactor: settings.replicationFactor.getValue(),
            isCollapseDocsWhenOpening: settings.collapseDocsWhenOpening.getValue(),
            isSendUsageStats: settings.sendUsageStats.getValue(),
        };
    });

    const { handleSubmit, control, formState, reset } = useForm<StudioGlobalConfigurationFormData>({
        resolver: studioGlobalConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncGlobalSettings.execute,
    });

    useDirtyFlag(formState.isDirty);
    const { reportEvent } = useEventsCollector();

    const onSave: SubmitHandler<StudioGlobalConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("studio-configuration-global", "save");
            const settings = await studioSettings.default.globalSettings();

            settings.environment.setValueLazy(formData.environment);
            settings.replicationFactor.setValueLazy(formData.replicationFactor);
            settings.collapseDocsWhenOpening.setValue(formData.isCollapseDocsWhenOpening);
            settings.sendUsageStats.setValueLazy(formData.isSendUsageStats);

            await settings.save();
            reset(formData);
        });
    };

    const onRefresh = async () => {
        reset(await asyncGlobalSettings.execute());
    };

    if (asyncGlobalSettings.status === "not-requested" || asyncGlobalSettings.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGlobalSettings.status === "error") {
        return <LoadError error="Unable to load studio configuration" refresh={onRefresh} />;
    }

    return (
        <Col lg="6" md="9" sm="12" className="gather-debug-info content-margin">
            <DevTool control={control} />
            <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                <ButtonWithSpinner
                    type="submit"
                    color="primary"
                    className="mb-3"
                    icon="save"
                    disabled={!formState.isDirty}
                    isSpinning={formState.isSubmitting}
                >
                    Save
                </ButtonWithSpinner>
                <Card id="popoverContainer">
                    <CardBody className="d-flex flex-center flex-column flex-wrap gap-4">
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Label className="mb-0 md-label">
                                Environment <Icon icon="info" color="info" id="EnvironmentInfo" />
                                <UncontrolledPopover
                                    target="EnvironmentInfo"
                                    placement="right"
                                    trigger="hover"
                                    container="popoverContainer"
                                >
                                    <PopoverBody>
                                        Change the studio environment tag. This does not affect settings or features.
                                    </PopoverBody>
                                </UncontrolledPopover>
                            </Label>
                            <FormSelect control={control} name="environment" options={environmentOptions}></FormSelect>
                        </InputGroup>
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Label className="mb-0 md-label">Default Replication Factor</Label>
                            <FormInput
                                control={control}
                                name="replicationFactor"
                                type="number"
                                placeholder="Cluster size (default)"
                            ></FormInput>
                        </InputGroup>
                    </CardBody>
                </Card>
                <Card className="mt-3">
                    <CardBody>
                        <div className="d-flex flex-column">
                            <FormSwitch control={control} name="isCollapseDocsWhenOpening">
                                Collapse documents when opening
                            </FormSwitch>
                            <FormSwitch control={control} name="isSendUsageStats" className="mt-2">
                                Help improve the Studio by gathering anonymous usage statistics
                            </FormSwitch>
                        </div>
                    </CardBody>
                </Card>
            </Form>
        </Col>
    );
}

const environmentOptions: SelectOption<StudioEnvironment>[] = allStudioEnvironments.map((x) => ({
    value: x,
    label: x,
}));
