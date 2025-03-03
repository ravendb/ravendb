import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSelect, FormSwitch, FormValidationMessage } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useServices } from "components/hooks/useServices";
import AdminLogsPersistInfoIcon from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsPersistInfoIcon";

import {
    adminLogsSelectors,
    adminLogsActions,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { exhaustiveStringTuple, tryHandleSubmit } from "components/utils/common";
import { useForm, useWatch, SubmitHandler, useFieldArray } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import Button from "react-bootstrap/Button";
import { AccordionItem, AccordionHeader, AccordionBody, Form, FormGroup, InputGroup, Label } from "reactstrap";
import * as yup from "yup";

type TrafficWatchChangeType = Raven.Client.Documents.Changes.TrafficWatchChangeType;
type TrafficWatchConfiguration =
    Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters;

export default function AdminLogsConfigTrafficWatch({ targetId }: { targetId: string }) {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();

    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const config = useAppSelector(adminLogsSelectors.configs).trafficWatchConfig;
    const databaseOptions = useAppSelector(databaseSelectors.allDatabases)
        .filter((x) => !x.isDisabled)
        .map((x) => ({ label: x.name, value: x.name }));

    const { control, formState, handleSubmit, reset, setValue } = useForm<FormData>({
        defaultValues: mapToFormDefaultValues(config),
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const isUsingHttps = location.protocol === "https:";

    const thumbprintFieldsArray = useFieldArray({
        control,
        name: "certificateThumbprints",
    });

    const certificateThumbprintsError =
        formState.errors?.certificateThumbprints?.message ?? formState.errors?.certificateThumbprints?.root?.message;

    const {
        isEnabled,
        isFilterByChangeType,
        isFilterByDatabaseName,
        isFilterByHttpMethod,
        isFilterByCertificateThumbprint,
        isFilterByStatusCode,
    } = useWatch({ control });

    const handleSave: SubmitHandler<FormData> = (data) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveTrafficWatchConfiguration(mapToDto(data, config));
            reset(data);
            dispatch(adminLogsActions.fetchConfigs());
            dispatch(adminLogsActions.isDiscSettingOpenToggled());
        });
    };

    return (
        <AccordionItem className="p-1 rounded-3">
            <AccordionHeader targetId={targetId}>Traffic watch</AccordionHeader>
            <AccordionBody accordionId={targetId}>
                <Form onSubmit={handleSubmit(handleSave)} key={targetId}>
                    <FormGroup>
                        <FormSwitch control={control} name="isEnabled">
                            Enable
                        </FormSwitch>
                    </FormGroup>
                    <Collapse in={isEnabled}>
                        <div>
                            <FormGroup>
                                <FormSwitch control={control} name="isFilterByChangeType">
                                    Filter by Change Type
                                </FormSwitch>
                                <Collapse in={isFilterByChangeType}>
                                    <InputGroup>
                                        <FormSelect
                                            control={control}
                                            name="changeTypes"
                                            placeholder="Select event types"
                                            options={changeTypesOptions}
                                            isMulti
                                        />
                                        <Button
                                            variant="secondary"
                                            type="button"
                                            onClick={() =>
                                                setValue(
                                                    "changeTypes",
                                                    changeTypesOptions.map((x) => x.value)
                                                )
                                            }
                                        >
                                            Select all
                                        </Button>
                                    </InputGroup>
                                </Collapse>
                            </FormGroup>
                            <FormGroup>
                                <FormSwitch control={control} name="isFilterByStatusCode">
                                    Filter by HTTP Status Code
                                </FormSwitch>
                                <Collapse in={isFilterByStatusCode}>
                                    <InputGroup>
                                        <FormSelect
                                            control={control}
                                            name="statusCodes"
                                            placeholder="Select status codes"
                                            options={statusCodesOptions}
                                            isMulti
                                        />
                                        <Button
                                            variant="secondary"
                                            type="button"
                                            onClick={() =>
                                                setValue(
                                                    "statusCodes",
                                                    statusCodesOptions.map((x) => x.value)
                                                )
                                            }
                                        >
                                            Select all
                                        </Button>
                                    </InputGroup>
                                </Collapse>
                            </FormGroup>
                            <FormGroup>
                                <FormSwitch control={control} name="isFilterByDatabaseName">
                                    Filter by Database
                                </FormSwitch>
                                <Collapse in={isFilterByDatabaseName}>
                                    <InputGroup>
                                        <FormSelect
                                            control={control}
                                            name="databaseNames"
                                            placeholder="Select databases"
                                            options={databaseOptions}
                                            isMulti
                                        />
                                        <Button
                                            variant="secondary"
                                            type="button"
                                            onClick={() =>
                                                setValue(
                                                    "databaseNames",
                                                    databaseOptions.map((x) => x.value)
                                                )
                                            }
                                        >
                                            Select all
                                        </Button>
                                    </InputGroup>
                                </Collapse>
                            </FormGroup>
                            <FormGroup>
                                <FormSwitch control={control} name="isFilterByHttpMethod">
                                    Filter by HTTP Method
                                </FormSwitch>
                                <Collapse in={isFilterByHttpMethod}>
                                    <InputGroup>
                                        <FormSelect
                                            control={control}
                                            name="httpMethods"
                                            placeholder="Select HTTP methods"
                                            options={httpMethodsOptions}
                                            isMulti
                                        />
                                        <Button
                                            variant="secondary"
                                            type="button"
                                            onClick={() =>
                                                setValue(
                                                    "httpMethods",
                                                    httpMethodsOptions.map((x) => x.value)
                                                )
                                            }
                                        >
                                            Select all
                                        </Button>
                                    </InputGroup>
                                </Collapse>
                            </FormGroup>
                            {isUsingHttps && (
                                <FormGroup>
                                    <FormSwitch control={control} name="isFilterByCertificateThumbprint">
                                        Filter by Certificate Thumbprint
                                    </FormSwitch>

                                    <Collapse in={isFilterByCertificateThumbprint}>
                                        <div>
                                            {thumbprintFieldsArray.fields.map((field, idx) => (
                                                <FormGroup key={field.id}>
                                                    <FormInput
                                                        type="text"
                                                        control={control}
                                                        name={`certificateThumbprints.${idx}.value`}
                                                        placeholder="Certificate Thumbprint"
                                                        addon={
                                                            <Button
                                                                type="button"
                                                                variant="link"
                                                                className="text-danger"
                                                                onClick={() => thumbprintFieldsArray.remove(idx)}
                                                            >
                                                                <Icon icon="trash" margin="m-0" />
                                                            </Button>
                                                        }
                                                    />
                                                </FormGroup>
                                            ))}
                                            {certificateThumbprintsError && (
                                                <FormValidationMessage>
                                                    {certificateThumbprintsError}
                                                </FormValidationMessage>
                                            )}
                                            <Button
                                                type="button"
                                                variant="outline-info"
                                                className="mt-1"
                                                onClick={() => thumbprintFieldsArray.append({ value: "" })}
                                            >
                                                <Icon icon="plus" />
                                                Add
                                            </Button>
                                        </div>
                                    </Collapse>
                                </FormGroup>
                            )}
                            <FormGroup>
                                <Label>Minimum Request Size</Label>
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="minimumRequestSizeInBytes"
                                    placeholder="Minimum Request Size"
                                    addon="bytes"
                                />
                            </FormGroup>
                            <FormGroup>
                                <Label>Minimum Request Duration</Label>
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="minimumRequestDurationInMs"
                                    placeholder="Minimum Duration"
                                    addon="ms"
                                />
                            </FormGroup>
                            <FormGroup>
                                <Label>Minimum Response Size</Label>
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="minimumResponseSizeInBytes"
                                    placeholder="Minimum Response Size"
                                    addon="bytes"
                                />
                            </FormGroup>
                            {!isCloud && (
                                <FormGroup>
                                    <FormSwitch control={control} name="isPersist">
                                        Save this configuration in <code>settings.json</code>
                                        <AdminLogsPersistInfoIcon />
                                    </FormSwitch>
                                </FormGroup>
                            )}
                        </div>
                    </Collapse>
                    <ButtonWithSpinner
                        type="submit"
                        variant="success"
                        className="ms-auto"
                        icon="save"
                        isSpinning={formState.isSubmitting}
                        disabled={!formState.isDirty}
                    >
                        Save
                    </ButtonWithSpinner>
                </Form>
            </AccordionBody>
        </AccordionItem>
    );
}

const schema = yup.object({
    isEnabled: yup.boolean(),
    isFilterByChangeType: yup.boolean(),
    changeTypes: yup
        .array()
        .of(yup.string<Raven.Client.Documents.Changes.TrafficWatchChangeType>())
        .when(["isEnabled", "isFilterByChangeType"], {
            is: (isEnabled: boolean, isFilterByChangeType: boolean) => isEnabled && isFilterByChangeType,
            then: (schema) => schema.min(1),
        }),
    isFilterByStatusCode: yup.boolean(),
    statusCodes: yup
        .array()
        .of(yup.number())
        .when(["isEnabled", "isFilterByStatusCode"], {
            is: (isEnabled: boolean, isFilterByStatusCode: boolean) => isEnabled && isFilterByStatusCode,
            then: (schema) => schema.min(1),
        }),
    isFilterByDatabaseName: yup.boolean(),
    databaseNames: yup
        .array()
        .of(yup.string())
        .when(["isEnabled", "isFilterByDatabaseName"], {
            is: (isEnabled: boolean, isFilterByDatabaseName: boolean) => isEnabled && isFilterByDatabaseName,
            then: (schema) => schema.min(1),
        }),
    isFilterByHttpMethod: yup.boolean(),
    httpMethods: yup
        .array()
        .of(yup.string())
        .when(["isEnabled", "isFilterByHttpMethod"], {
            is: (isEnabled: boolean, isFilterByHttpMethod: boolean) => isEnabled && isFilterByHttpMethod,
            then: (schema) => schema.min(1),
        }),
    isFilterByCertificateThumbprint: yup.boolean(),
    certificateThumbprints: yup
        .array()
        .of(
            yup.object({
                value: yup.string().when(["isEnabled", "isFilterByCertificateThumbprint"], {
                    is: (isEnabled: boolean, isFilterByCertificateThumbprint: boolean) =>
                        isEnabled && isFilterByCertificateThumbprint,
                    then: (schema) => schema.required(),
                }),
            })
        )
        .when(["isEnabled", "isFilterByCertificateThumbprint"], {
            is: (isEnabled: boolean, isFilterByCertificateThumbprint: boolean) =>
                isEnabled && isFilterByCertificateThumbprint,
            then: (schema) => schema.min(1),
        }),
    minimumRequestSizeInBytes: yup
        .number()
        .integer()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.min(0).required(),
        }),
    minimumRequestDurationInMs: yup
        .number()
        .integer()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.min(0).required(),
        }),
    minimumResponseSizeInBytes: yup
        .number()
        .integer()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.min(0).required(),
        }),
    isPersist: yup.boolean(),
});

type FormData = yup.InferType<typeof schema>;

function mapToFormDefaultValues(config: Omit<TrafficWatchConfiguration, "Persist">): FormData {
    return {
        isEnabled: config.TrafficWatchMode === "ToLogFile",
        isFilterByChangeType: config.ChangeTypes?.length > 0,
        changeTypes: config.ChangeTypes ?? [],
        isFilterByStatusCode: config.StatusCodes?.length > 0,
        statusCodes: config.StatusCodes ?? [],
        isFilterByDatabaseName: config.Databases?.length > 0,
        databaseNames: config.Databases ?? [],
        isFilterByHttpMethod: config.HttpMethods?.length > 0,
        httpMethods: config.HttpMethods ?? [],
        isFilterByCertificateThumbprint: config.CertificateThumbprints?.length > 0,
        certificateThumbprints: config.CertificateThumbprints?.map((x) => ({ value: x })) ?? [],
        minimumRequestSizeInBytes: config.MinimumRequestSizeInBytes,
        minimumRequestDurationInMs: config.MinimumDurationInMs,
        minimumResponseSizeInBytes: config.MinimumResponseSizeInBytes,
        isPersist: false,
    };
}

function mapToDto(data: FormData, config: Omit<TrafficWatchConfiguration, "Persist">): TrafficWatchConfiguration {
    if (data.isEnabled) {
        return {
            TrafficWatchMode: "ToLogFile",
            ChangeTypes: data.isFilterByChangeType ? data.changeTypes : null,
            StatusCodes: data.isFilterByStatusCode ? data.statusCodes : null,
            Databases: data.isFilterByDatabaseName ? data.databaseNames : null,
            HttpMethods: data.isFilterByHttpMethod ? data.httpMethods : null,
            CertificateThumbprints: data.isFilterByCertificateThumbprint
                ? data.certificateThumbprints.map((x) => x.value)
                : null,
            MinimumRequestSizeInBytes: data.minimumRequestSizeInBytes,
            MinimumDurationInMs: data.minimumRequestDurationInMs,
            MinimumResponseSizeInBytes: data.minimumResponseSizeInBytes,
            Persist: data.isPersist,
        };
    }

    return {
        ...config,
        TrafficWatchMode: "Off",
        Persist: false,
    };
}

const httpMethodsOptions: SelectOption[] = ["GET", "POST", "PUT", "DELETE", "HEAD"].map((x) => ({
    label: x,
    value: x,
}));

const changeTypesOptions: SelectOption<TrafficWatchChangeType>[] = exhaustiveStringTuple<
    Exclude<TrafficWatchChangeType, "None">
>()(
    "BulkDocs",
    "ClusterCommands",
    "Counters",
    "Documents",
    "Hilo",
    "Index",
    "MultiGet",
    "Notifications",
    "Operations",
    "Queries",
    "Streams",
    "Subscriptions",
    "TimeSeries"
).map((x) => ({ label: x, value: x }));

const statusCodesOptions: SelectOption<number>[] = [
    101, 200, 201, 202, 203, 204, 301, 302, 304, 307, 308, 400, 401, 403, 404, 405, 408, 409, 415, 429, 500, 501, 502,
    503, 504, 505,
].map((x) => ({ label: x.toString(), value: x }));
