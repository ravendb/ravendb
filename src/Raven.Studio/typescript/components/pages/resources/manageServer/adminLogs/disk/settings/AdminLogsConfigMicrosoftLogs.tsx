import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormSelect, FormCheckbox } from "components/common/Form";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useServices } from "components/hooks/useServices";
import AdminLogsConfigTableValue from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsConfigTableValue";
import AdminLogsPersistInfoIcon from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsPersistInfoIcon";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { logLevelOptions, tryHandleSubmit } from "components/utils/common";
import { SubmitHandler, useForm } from "react-hook-form";
import {
    AccordionBody,
    AccordionHeader,
    AccordionItem,
    Form,
    FormGroup,
    Label,
    Table,
    UncontrolledPopover,
} from "reactstrap";
import * as yup from "yup";
import { Icon } from "components/common/Icon";

type MicrosoftLogsConfig = Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult["MicrosoftLogs"];

interface AdminLogsConfigMicrosoftLogsProps {
    targetId: string;
}

export default function AdminLogsConfigMicrosoftLogs({ targetId }: AdminLogsConfigMicrosoftLogsProps) {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();

    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.MicrosoftLogs;
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const { control, formState, handleSubmit, reset } = useForm<FormData>({
        defaultValues: mapToFormDefaultValues(config),
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const handleSave: SubmitHandler<FormData> = (data) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveAdminLogsConfiguration(mapToDto(data));
            reset(data);
            dispatch(adminLogsActions.fetchConfigs());
            dispatch(adminLogsActions.isDiscSettingOpenToggled());
        });
    };

    return (
        <AccordionItem className="p-1 rounded-3">
            <AccordionHeader targetId={targetId}>Microsoft logs</AccordionHeader>
            <AccordionBody accordionId={targetId}>
                <h5 className="text-center text-muted text-uppercase">Set min level</h5>
                <Form onSubmit={handleSubmit(handleSave)} key={targetId}>
                    <FormGroup>
                        <Label>Current Minimum Level</Label>
                        <FormSelect control={control} name="minLevel" options={logLevelOptions} />
                        {!isCloud && (
                            <FormCheckbox control={control} name="isPersist" className="mt-1">
                                Save the minimum level in <code>settings.json</code>
                                <AdminLogsPersistInfoIcon />
                            </FormCheckbox>
                        )}
                    </FormGroup>
                    <ButtonWithSpinner
                        type="submit"
                        color="success"
                        className="ms-auto"
                        icon="save"
                        isSpinning={formState.isSubmitting}
                        disabled={!formState.isDirty}
                    >
                        Save
                    </ButtonWithSpinner>
                </Form>
                <h5 className="text-center text-muted text-uppercase">
                    Read-only
                    <span id="read-only-tooltip-for-ms-logs">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </span>
                    <UncontrolledPopover target="read-only-tooltip-for-ms-logs" trigger="hover" className="bs5">
                        <div className="p-3">
                            This setting is not editable here but can be configured through the server configuration.
                        </div>
                    </UncontrolledPopover>
                </h5>
                <Table className="m-0">
                    <tbody>
                        <tr>
                            <td>Minimum Level</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MinLevel} />
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </AccordionBody>
        </AccordionItem>
    );
}

const schema = yup.object({
    minLevel: yup.string<Sparrow.Logging.LogLevel>().nullable().required(),
    isPersist: yup.boolean(),
});

type FormData = yup.InferType<typeof schema>;

function mapToFormDefaultValues(config: MicrosoftLogsConfig): FormData {
    return {
        minLevel: config.CurrentMinLevel,
        isPersist: false,
    };
}

function mapToDto(
    data: FormData
): Partial<Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters> {
    return {
        MicrosoftLogs: {
            MinLevel: data.minLevel,
        },
        Persist: data.isPersist,
    };
}
