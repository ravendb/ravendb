import Collapse from "react-bootstrap/Collapse";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import { FormCheckbox, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { exhaustiveStringTuple, tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import * as yup from "yup";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import AdminLogsPersistInfoIcon from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsPersistInfoIcon";
import Accordion from "react-bootstrap/Accordion";

type EventListenerConfiguration = Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration;

export default function AdminLogsConfigEventListener({ targetId }: { targetId: string }) {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();

    const config = useAppSelector(adminLogsSelectors.configs).eventListenerConfig;
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const { control, formState, handleSubmit, reset, setValue } = useForm<FormData>({
        defaultValues: mapToFormDefaultValues(config),
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const { isEnabled } = useWatch({ control });

    const handleSave: SubmitHandler<FormData> = (data) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveEventListenerConfiguration(mapToDto(data, config));
            reset(data);
            dispatch(adminLogsActions.fetchConfigs());
            dispatch(adminLogsActions.isDiscSettingOpenToggled());
        });
    };

    return (
        <Accordion.Item eventKey={targetId} className="p-1 rounded-3">
            <Accordion.Header>Event listener</Accordion.Header>
            <Accordion.Body>
                <Form onSubmit={handleSubmit(handleSave)} key={targetId}>
                    <FormGroup>
                        <FormSwitch control={control} name="isEnabled">
                            Enable
                        </FormSwitch>
                    </FormGroup>
                    <Collapse in={isEnabled}>
                        <div>
                            <FormGroup>
                                <FormLabel>Event Types</FormLabel>
                                <InputGroup>
                                    <FormSelect
                                        control={control}
                                        name="eventTypes"
                                        placeholder="Select event types"
                                        options={allEventTypeOptions}
                                        isMulti
                                    />
                                    <Button
                                        variant="secondary"
                                        type="button"
                                        onClick={() =>
                                            setValue(
                                                "eventTypes",
                                                allEventTypeOptions.map((x) => x.value)
                                            )
                                        }
                                    >
                                        Select all
                                    </Button>
                                </InputGroup>
                            </FormGroup>
                            <FormGroup>
                                <FormLabel>Minimum Duration</FormLabel>
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="minimumDurationInMs"
                                    placeholder="Minimum duration in ms"
                                    addon="ms"
                                />
                            </FormGroup>
                            <Row>
                                <Col>
                                    <FormGroup>
                                        <FormLabel>Allocations Logging Interval</FormLabel>
                                        <FormInput
                                            type="number"
                                            control={control}
                                            name="allocationsLoggingIntervalInMs"
                                            placeholder="Allocations Logging Interval"
                                            addon="ms"
                                        />
                                    </FormGroup>
                                </Col>
                                <Col>
                                    <FormGroup>
                                        <FormLabel>Allocations Logging Count</FormLabel>
                                        <FormInput
                                            type="number"
                                            control={control}
                                            name="allocationsLoggingCount"
                                            placeholder="Allocations Logging Count"
                                        />
                                    </FormGroup>
                                </Col>
                            </Row>
                            {!isCloud && (
                                <FormCheckbox control={control} name="isPersist">
                                    Save this configuration in <code>settings.json</code>
                                    <AdminLogsPersistInfoIcon />
                                </FormCheckbox>
                            )}
                        </div>
                    </Collapse>
                    <ButtonWithSpinner
                        type="submit"
                        variant="success"
                        icon="save"
                        className="ms-auto"
                        isSpinning={formState.isSubmitting}
                        disabled={!formState.isDirty}
                    >
                        Save
                    </ButtonWithSpinner>
                </Form>
            </Accordion.Body>
        </Accordion.Item>
    );
}

const schema = yup.object({
    isEnabled: yup.boolean(),
    eventTypes: yup
        .array()
        .of(yup.string<Raven.Server.EventListener.EventType>())
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.min(1).required(),
        }),
    minimumDurationInMs: yup
        .number()
        .integer()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.min(0).required(),
        }),
    allocationsLoggingCount: yup
        .number()
        .integer()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.min(0).required(),
        }),
    allocationsLoggingIntervalInMs: yup
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

function mapToFormDefaultValues(config: Omit<EventListenerConfiguration, "Persist">): FormData {
    return {
        isEnabled: config.EventListenerMode === "ToLogFile",
        eventTypes: config.EventTypes ?? [],
        minimumDurationInMs: config.MinimumDurationInMs,
        allocationsLoggingCount: config.AllocationsLoggingCount,
        allocationsLoggingIntervalInMs: config.AllocationsLoggingIntervalInMs,
        isPersist: false,
    };
}

function mapToDto(data: FormData, config: Omit<EventListenerConfiguration, "Persist">): EventListenerConfiguration {
    if (data.isEnabled) {
        return {
            EventListenerMode: "ToLogFile",
            EventTypes: data.eventTypes,
            AllocationsLoggingCount: data.allocationsLoggingCount,
            AllocationsLoggingIntervalInMs: data.allocationsLoggingIntervalInMs,
            MinimumDurationInMs: data.minimumDurationInMs,
            Persist: data.isPersist,
        };
    }

    return {
        ...config,
        EventListenerMode: "Off",
        Persist: false,
    };
}

const allEventTypeOptions = exhaustiveStringTuple<Raven.Server.EventListener.EventType>()(
    "Allocations",
    "Contention",
    "GC",
    "GCCreateConcurrentThread_V1",
    "GCFinalizers",
    "GCRestart",
    "GCSuspend",
    "GCJoin",
    "GCHeapStats",
    "ThreadCreated",
    "ThreadCreating",
    "ThreadPoolMinMaxThreads",
    "ThreadPoolWorkerThreadAdjustment",
    "ThreadPoolWorkerThreadAdjustmentSample",
    "ThreadPoolWorkerThreadAdjustmentStats",
    "ThreadPoolWorkerThreadStart",
    "ThreadPoolWorkerThreadStop",
    "ThreadPoolWorkerThreadWait",
    "ThreadRunning"
).map((x) => ({ label: x, value: x }));
