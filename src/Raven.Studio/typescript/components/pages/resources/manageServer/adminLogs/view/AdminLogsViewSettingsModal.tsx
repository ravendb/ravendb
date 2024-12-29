import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useServices } from "components/hooks/useServices";
import AdminLogsFilterField from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsFilterField";
import { adminLogsUtils } from "components/pages/resources/manageServer/adminLogs/common/adminLogsUtils";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { logFilterActionOptions, tryHandleSubmit } from "components/utils/common";
import { SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { Button, CloseButton, Form, FormGroup, Label, Modal, ModalBody, UncontrolledPopover } from "reactstrap";
import * as yup from "yup";

type AdminLogsConfig = Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult["AdminLogs"];

export default function AdminLogsViewSettingsModal() {
    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.AdminLogs;

    const { control, handleSubmit, reset, formState } = useForm<AdminLogsViewSettingsFormData>({
        defaultValues: mapToFormDefaultValues(config),
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const filterFieldArray = useFieldArray({
        control,
        name: "filters",
    });

    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();

    const handleSave: SubmitHandler<AdminLogsViewSettingsFormData> = (data) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveAdminLogsConfiguration(mapToDto(data, config));
            reset(data);
            dispatch(adminLogsActions.fetchConfigs());
            dispatch(adminLogsActions.isViewSettingOpenToggled());
        });
    };

    return (
        <Modal isOpen wrapClassName="bs5" centered size="lg">
            <ModalBody>
                <div className="d-flex">
                    <h3>
                        <Icon icon="client" addon="settings" />
                        Settings - logs on this view
                    </h3>
                    <CloseButton
                        className="ms-auto"
                        onClick={() => dispatch(adminLogsActions.isViewSettingOpenToggled())}
                    />
                </div>

                <Form onSubmit={handleSubmit(handleSave)}>
                    <FormGroup>
                        <Label>
                            Default Filter Action
                            <span id="default-filter-action-for-view">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </span>
                            <UncontrolledPopover
                                target="default-filter-action-for-view"
                                trigger="hover"
                                className="bs5"
                            >
                                <div className="p-3">
                                    <p className="mb-1">
                                        This action does <strong>Not apply</strong> when no filters are defined.
                                        <br />
                                        This action <strong>applies</strong> in the following cases:
                                    </p>
                                    <ul className="mb-0">
                                        <li className="mb-1">When a log entry does Not match any defined filter.</li>
                                        <li>
                                            When a log entry matches a filter with a <code>Neutral</code> action,
                                            provided that no subsequent filters apply.
                                        </li>
                                    </ul>
                                </div>
                            </UncontrolledPopover>
                        </Label>
                        <FormSelect
                            control={control}
                            name="logFilterDefaultAction"
                            options={logFilterActionOptions}
                            isDisabled={filterFieldArray.fields.length === 0}
                        />
                    </FormGroup>
                    <FormGroup className="vstack">
                        <Label>Filters</Label>
                        <div className="vstack gap-1 mb-1">
                            {filterFieldArray.fields.map((field, idx) => (
                                <AdminLogsFilterField
                                    key={field.id}
                                    control={control}
                                    idx={idx}
                                    remove={() => filterFieldArray.remove(idx)}
                                />
                            ))}
                        </div>
                        <Button
                            type="button"
                            color="info"
                            className="w-fit-content"
                            onClick={() => filterFieldArray.append(adminLogsUtils.initialFilter)}
                        >
                            <Icon icon="plus" />
                            Add Filter
                        </Button>
                    </FormGroup>
                    <div className="d-flex justify-content-end gap-2">
                        <Button type="button" onClick={() => dispatch(adminLogsActions.isViewSettingOpenToggled())}>
                            <Icon icon="cancel" />
                            Close
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            icon="save"
                            color="success"
                            isSpinning={formState.isSubmitting}
                            disabled={!formState.isDirty}
                        >
                            Save
                        </ButtonWithSpinner>
                    </div>
                </Form>
            </ModalBody>
        </Modal>
    );
}

const schema = yup.object({
    logFilterDefaultAction: yup.string<Sparrow.Logging.LogFilterAction>().required(),
    filters: adminLogsUtils.filtersSchema,
});

export type AdminLogsViewSettingsFormData = yup.InferType<typeof schema>;

function mapToFormDefaultValues(config: AdminLogsConfig) {
    if (!config) {
        return {
            logFilterDefaultAction: null,
            filters: [],
        };
    }

    return {
        logFilterDefaultAction: config.CurrentLogFilterDefaultAction,
        filters: config.CurrentFilters.map((x) => ({
            minLevel: x.MinLevel,
            maxLevel: x.MaxLevel,
            condition: x.Condition,
            action: x.Action,
        })),
    };
}

function mapToDto(formData: AdminLogsViewSettingsFormData, config: AdminLogsConfig) {
    return {
        AdminLogs: {
            MinLevel: config.CurrentMinLevel,
            LogFilterDefaultAction: formData.logFilterDefaultAction,
            Filters: formData.filters.map((x) => ({
                MinLevel: x.minLevel,
                MaxLevel: x.maxLevel,
                Condition: x.condition,
                Action: x.action,
            })),
        },
    };
}
