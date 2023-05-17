import React from "react";
import {
    Button,
    Card,
    CardBody,
    Col,
    Form,
    InputGroup,
    Label,
    PopoverBody,
    Spinner,
    UncontrolledPopover,
} from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import {
    StudioConfigurationFormData,
    studioConfigurationYupResolver,
} from "../../../../common/studioConfiguration/StudioConfigurationValidation";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import StudioConfigurationUtils from "components/common/studioConfiguration/StudioConfigurationUtils";
import useStudioConfigurationFormController from "components/common/studioConfiguration/useStudioConfigurationFormController";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";

export default function StudioGlobalConfiguration() {
    const { manageServerService } = useServices();
    const asyncGetStudioConfiguration = useAsyncCallback(manageServerService.getStudioConfiguration);

    const { handleSubmit, control, formState, setValue, reset } = useForm<StudioConfigurationFormData>({
        resolver: studioConfigurationYupResolver,
        mode: "all",
        defaultValues: async () =>
            StudioConfigurationUtils.mapToFormData(await asyncGetStudioConfiguration.execute(), true),
    });

    const formValues = useStudioConfigurationFormController(control, setValue);

    const onSave: SubmitHandler<StudioConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveStudioConfiguration(StudioConfigurationUtils.mapToDto(formData, true));
            reset(null, { keepValues: true });
        });
    };

    const onRefresh = async () => {
        reset(StudioConfigurationUtils.mapToFormData(await asyncGetStudioConfiguration.execute(), true));
    };

    if (asyncGetStudioConfiguration.loading) {
        return <LoadingView />;
    }

    if (asyncGetStudioConfiguration.error) {
        return <LoadError error="Unable to load studio configuration" refresh={onRefresh} />;
    }

    return (
        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
            <Col lg="6" md="9" sm="12" className="gather-debug-info">
                <Button
                    type="submit"
                    color="primary"
                    className="mb-3"
                    disabled={formState.isSubmitting || !formState.isDirty}
                >
                    {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <Icon icon="save" />}
                    Save
                </Button>
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
                            <FormSelect
                                control={control}
                                name="environmentTagValue"
                                options={StudioConfigurationUtils.getEnvironmentTagOptions()}
                                className="w-100"
                            ></FormSelect>
                        </InputGroup>
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Label className="mb-0 md-label">Default Replication Factor</Label>
                            <FormInput
                                control={control}
                                name="defaultReplicationFactorValue"
                                type="number"
                                placeholder="Cluster size (default)"
                                className="w-100"
                            ></FormInput>
                        </InputGroup>
                    </CardBody>
                </Card>
                <Card className="mt-3">
                    <CardBody>
                        <div className="d-flex flex-column">
                            <FormSwitch control={control} name="collapseDocsWhenOpeningEnabled">
                                Collapse documents when opening
                            </FormSwitch>
                            <FormSwitch control={control} name="sendAnonymousUsageDataEnabled" className="mt-2">
                                Help improve the Studio by gathering anonymous usage statistics
                            </FormSwitch>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </Form>
    );
}
