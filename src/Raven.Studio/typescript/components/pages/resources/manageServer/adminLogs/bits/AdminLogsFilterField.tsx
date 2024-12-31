import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormSelect, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { SelectOption } from "components/common/select/Select";
import { AdminLogsConfigLogsFormData } from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigLogs";
import { AdminLogsViewSettingsFormData } from "components/pages/resources/manageServer/adminLogs/view/AdminLogsViewSettingsModal";
import { logLevelOptions, logFilterActionOptions, logLevelRelevances } from "components/utils/common";
import { Control, useWatch } from "react-hook-form";
import { components, OptionProps } from "react-select";
import { Row, Col, FormGroup, Label, Button, UncontrolledPopover, Card, InputGroup } from "reactstrap";

type FormData = AdminLogsViewSettingsFormData | AdminLogsConfigLogsFormData;

interface LevelOptionProps extends SelectOption {
    isDisabled?: boolean;
    level?: "max" | "min";
}

interface AdminLogsFilterFieldProps {
    control: Control<FormData>;
    idx: number;
    remove: () => void;
}

export default function AdminLogsFilterField({ control, idx, remove }: AdminLogsFilterFieldProps) {
    const formValues = useWatch({ control });

    const maxLevel = formValues.filters[idx].maxLevel;
    const minLevel = formValues.filters[idx].minLevel;

    const getMinLevelOptions = (): LevelOptionProps[] => {
        if (maxLevel) {
            return logLevelOptions.map((option) => ({
                ...option,
                level: "min",
                isDisabled: logLevelRelevances[option.value] > logLevelRelevances[maxLevel],
            }));
        }

        return logLevelOptions;
    };

    const getMaxLevelOptions = (): LevelOptionProps[] => {
        if (minLevel) {
            return logLevelOptions.map((option) => ({
                ...option,
                level: "max",
                isDisabled: logLevelRelevances[option.value] < logLevelRelevances[minLevel],
            }));
        }

        return logLevelOptions;
    };

    return (
        <Card color="faded-info" className="p-3 rounded">
            <Row>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <Label>Minimum level</Label>
                        <FormSelect
                            control={control}
                            name={`filters.${idx}.minLevel`}
                            options={getMinLevelOptions()}
                            components={{ Option: LevelOption }}
                        />
                    </FormGroup>
                </Col>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <Label>Maximum level</Label>
                        <FormSelect
                            control={control}
                            name={`filters.${idx}.maxLevel`}
                            options={getMaxLevelOptions()}
                            components={{ Option: LevelOption }}
                        />
                    </FormGroup>
                </Col>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <Label>
                            Action
                            <span id="filter-action">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </span>
                            <UncontrolledPopover target="filter-action" trigger="hover" className="bs5" placement="top">
                                <div className="p-3">
                                    <p>
                                        The selected action will apply to all log entries that match the filter&apos;s
                                        logging-rules (condition + min + max levels):
                                    </p>
                                    <ul className="mb-1">
                                        <li className="mb-1">
                                            <code>Ignore</code> - The log entry will Not be logged.
                                        </li>
                                        <li className="mb-1">
                                            <code>IgnoreFinal</code> - The log entry will Not be logged.
                                            <br />
                                            Any subsequent filters with the same logging-rules as this filter will be
                                            ignored.
                                        </li>
                                        <li className="mb-1">
                                            <code>Log</code> - The log entry will be logged.
                                        </li>
                                        <li className="mb-1">
                                            <code>LogFinal</code> - The log entry will be logged.
                                            <br />
                                            Any subsequent filters with the same logging-rules as this filter will be
                                            ignored.
                                        </li>
                                        <li>
                                            <code>Neutral</code> - The action to take is deferred to the next filter
                                            that matches the log entry. If no other filter matches, the &quot;Default
                                            Filter Action&quot; will be applied.
                                        </li>
                                    </ul>
                                </div>
                            </UncontrolledPopover>
                        </Label>
                        <FormSelect control={control} name={`filters.${idx}.action`} options={logFilterActionOptions} />
                    </FormGroup>
                </Col>
            </Row>
            <div className="flex-grow-1 mb-0">
                <Label className="d-flex">
                    Condition
                    <div id="condition-tooltip">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </div>
                    <UncontrolledPopover target="condition-tooltip" trigger="hover" className="bs5">
                        <div className="p-3">
                            More info here:
                            <br />
                            <a href="https://github.com/NLog/NLog/wiki/When-filter#conditions" target="_blank">
                                github.com/NLog/NLog/wiki/When-filter#conditions
                            </a>
                        </div>
                    </UncontrolledPopover>
                </Label>
                <InputGroup>
                    <FormInput
                        control={control}
                        name={`filters.${idx}.condition`}
                        type="text"
                        className="border-top-right-radius-none border-bottom-right-radius-none"
                    />
                    <Button type="button" color="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </InputGroup>
            </div>
        </Card>
    );
}

export function LevelOption(props: OptionProps<LevelOptionProps>) {
    const { data } = props;

    const getDisabledReason = (): string => {
        if (!data.isDisabled) {
            return null;
        }
        if (data.level === "min") {
            return "The minimum level cannot be higher than the maximum level";
        }
        if (data.level === "max") {
            return "The maximum level cannot be lower than the minimum level";
        }
    };

    return (
        <ConditionalPopover
            conditions={{
                isActive: data.isDisabled,
                message: getDisabledReason(),
            }}
            popoverPlacement="top"
            className="w-100"
        >
            <components.Option {...props}>{data.label}</components.Option>
        </ConditionalPopover>
    );
}
