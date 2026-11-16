import { useState } from "react";
import moment from "moment";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Spinner from "react-bootstrap/Spinner";
import { Control, FieldPath, FieldValues, useController } from "react-hook-form";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import Select, { SelectOption } from "components/common/select/Select";
import RadioButtons from "components/common/toggles/RadioButtons";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import genUtils from "common/generalUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { CronBasicSchedule, cronEditorUtils } from "./cronEditorUtils";
import "./FormCronEditor.scss";

type ScheduleType = "basic" | "advanced";

interface FormCronEditorProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    name: TName;
    databaseName?: string;
    taskId?: number;
    isFullBackup?: boolean;
    onServerValidation?: CronServerValidationHandler;
    isDisabled?: boolean;
    isSummaryHidden?: boolean;
}

export type CronServerValidationHandler = (expression: string, errorMessage: string | null) => void;

export default function FormCronEditor<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    name,
    databaseName,
    taskId,
    isFullBackup,
    onServerValidation,
    isDisabled,
    isSummaryHidden,
}: FormCronEditorProps<TFieldValues, TName>) {
    const {
        field: { value, onChange },
        fieldState: { error },
    } = useController({ control, name });

    const expression: string = value ?? "";
    const basicSchedule = cronEditorUtils.parseBasicSchedule(expression);

    const [scheduleType, setScheduleType] = useState<ScheduleType>(!expression || basicSchedule ? "basic" : "advanced");
    const { value: isSyntaxChartVisible, toggle: toggleSyntaxChart } = useBoolean(false);
    const t = useStudioTranslation("cronEditor");

    const changeBasicSchedule = (changes: Partial<CronBasicSchedule>) => {
        const current = basicSchedule ?? cronEditorUtils.getDefaultBasicSchedule(changes.period ?? "day");
        onChange(cronEditorUtils.buildCronExpression({ ...current, ...changes }));
    };

    const period = basicSchedule?.period;
    const isTimeVisible = period === "day" || period === "week" || period === "month" || period === "year";

    return (
        <div className="cron-editor vstack gap-3">
            <FormGroup marginClass="m-0" className="vstack gap-1">
                <FormLabel className="mb-0">{t("scheduleType")}</FormLabel>
                <RadioButtons<ScheduleType>
                    aria-label={t("scheduleType")}
                    items={[
                        { value: "basic", label: t("basic") },
                        { value: "advanced", label: t("advanced"), helpMessage: t("advancedHelp") },
                    ]}
                    selected={scheduleType}
                    onSelect={setScheduleType}
                    isDisabled={isDisabled}
                />
            </FormGroup>
            {scheduleType === "basic" ? (
                <FormGroup marginClass="m-0" className="vstack gap-1">
                    <FormLabel className="mb-0">{t("runEvery")}</FormLabel>
                    <BasicSelect
                        isDisabled={isDisabled}
                        aria-label={t("runEvery")}
                        placeholder={t("intervalPlaceholder")}
                        options={cronEditorUtils.getPeriodOptions()}
                        value={period}
                        onChange={(period) => changeBasicSchedule({ period })}
                    />
                    {period === "hour" && (
                        <BasicSelect
                            isDisabled={isDisabled}
                            aria-label={t("minutesPastHour")}
                            options={cronEditorUtils.getMinutePastHourOptions(basicSchedule.minute)}
                            value={basicSchedule.minute}
                            onChange={(minute) => changeBasicSchedule({ minute })}
                            isCustomValue={cronEditorUtils.isCustomMinute}
                            formatValue={(minute) => t("minutesPastHourValue", { minute })}
                        />
                    )}
                    {period === "week" && (
                        <BasicSelect
                            isDisabled={isDisabled}
                            aria-label={t("dayOfWeek")}
                            options={cronEditorUtils.getDayOfWeekOptions()}
                            value={basicSchedule.dayOfWeek}
                            onChange={(dayOfWeek) => changeBasicSchedule({ dayOfWeek })}
                            formatValue={(day) => t("dayOfWeekValue", { day })}
                        />
                    )}
                    {(period === "month" || period === "year") && (
                        <BasicSelect
                            isDisabled={isDisabled}
                            aria-label={t("dayOfMonth")}
                            options={cronEditorUtils.DAY_OF_MONTH_OPTIONS}
                            value={basicSchedule.dayOfMonth}
                            onChange={(dayOfMonth) => changeBasicSchedule({ dayOfMonth })}
                            formatValue={(day) => t("dayOfMonthValue", { day })}
                        />
                    )}
                    {period === "year" && (
                        <BasicSelect
                            isDisabled={isDisabled}
                            aria-label={t("month")}
                            options={cronEditorUtils.getMonthOptions()}
                            value={basicSchedule.month}
                            onChange={(month) => changeBasicSchedule({ month })}
                            formatValue={(month) => t("monthValue", { month })}
                        />
                    )}
                    {isTimeVisible && (
                        <div className="hstack gap-2">
                            <div className="flex-grow-1">
                                <BasicSelect
                                    isDisabled={isDisabled}
                                    aria-label={t("hour")}
                                    options={cronEditorUtils.getHourOptions()}
                                    value={basicSchedule.hour}
                                    onChange={(hour) => changeBasicSchedule({ hour })}
                                    formatValue={(hour) => t("hourValue", { hour })}
                                />
                            </div>
                            <span className="text-muted">:</span>
                            <div className="flex-grow-1">
                                <BasicSelect
                                    isDisabled={isDisabled}
                                    aria-label={t("minute")}
                                    options={cronEditorUtils.getMinuteOptions(basicSchedule.minute)}
                                    value={basicSchedule.minute}
                                    onChange={(minute) => changeBasicSchedule({ minute })}
                                    isCustomValue={cronEditorUtils.isCustomMinute}
                                />
                            </div>
                        </div>
                    )}
                    {!period && expression && (
                        <div className="small text-warning">
                            <StudioTrans
                                ns="cronEditor"
                                i18nKey="advancedOnly"
                                components={{ code: <code /> }}
                                options={{ expression }}
                            />
                        </div>
                    )}
                    {error && !expression && <div className="small text-danger">{error.message}</div>}
                </FormGroup>
            ) : (
                <div className="vstack gap-2">
                    <div className="vstack gap-1">
                        <Button
                            variant="link"
                            className="cron-editor-link md-label text-decoration-none align-self-start p-0"
                            onClick={toggleSyntaxChart}
                            aria-expanded={isSyntaxChartVisible}
                        >
                            {isSyntaxChartVisible ? t("hideSyntaxChart") : t("viewSyntaxChart")}
                            <Icon icon={isSyntaxChartVisible ? "collapse-vertical" : "expand-vertical"} margin="ms-1" />
                        </Button>
                        <Collapse in={isSyntaxChartVisible}>
                            <div>
                                <pre className="cron-editor-syntax-chart">{SYNTAX_CHART}</pre>
                            </div>
                        </Collapse>
                    </div>
                    <FormGroup marginClass="m-0">
                        <FormLabel>{t("expression")}</FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name={name}
                            placeholder={t("expressionPlaceholder")}
                            autoComplete="off"
                            disabled={isDisabled}
                        />
                    </FormGroup>
                </div>
            )}
            {expression && !isSummaryHidden && (
                <CronEditorSummary
                    expression={expression}
                    databaseName={databaseName}
                    taskId={taskId}
                    isFullBackup={isFullBackup}
                    onServerValidation={onServerValidation}
                />
            )}
        </div>
    );
}

interface BasicSelectProps<T> {
    options: SelectOption<T>[];
    value: T;
    onChange: (value: T) => void;
    formatValue?: (label: string) => string;
    isCustomValue?: (value: T) => boolean;
    placeholder?: string;
    isDisabled?: boolean;
    "aria-label": string;
}

function BasicSelect<T>({
    options,
    value,
    onChange,
    formatValue,
    isCustomValue,
    placeholder,
    isDisabled,
    "aria-label": ariaLabel,
}: BasicSelectProps<T>) {
    const t = useStudioTranslation("cronEditor");

    const formatOptionLabel = (option: SelectOption<T>, context: "menu" | "value") => {
        if (context === "value") {
            return formatValue ? formatValue(option.label) : option.label;
        }
        return isCustomValue?.(option.value) ? t("customOption", { label: option.label }) : option.label;
    };

    return (
        <Select
            options={options}
            value={options.find((x) => x.value === value) ?? null}
            onChange={(option: SelectOption<T>) => onChange(option.value)}
            formatOptionLabel={(option, { context }) => formatOptionLabel(option, context)}
            placeholder={placeholder}
            isDisabled={isDisabled}
            aria-label={ariaLabel}
        />
    );
}

interface CronEditorSummaryProps {
    expression: string;
    databaseName?: string;
    taskId?: number;
    isFullBackup?: boolean;
    onServerValidation?: CronServerValidationHandler;
}

function CronEditorSummary({
    expression,
    databaseName,
    taskId,
    isFullBackup,
    onServerValidation,
}: CronEditorSummaryProps) {
    const { tasksService } = useServices();
    const t = useStudioTranslation("cronEditor");

    const validationError = cronEditorUtils.getValidationError(expression);
    const description = cronEditorUtils.getDescription(expression);

    const asyncNextOccurrence = useAsyncDebounce(async () => {
        if (validationError) {
            return null;
        }
        const occurrence = await tasksService.getCronExpressionOccurrence(
            expression,
            databaseName,
            taskId,
            isFullBackup
        );
        onServerValidation?.(expression, occurrence.IsValid ? null : occurrence.ErrorMessage);
        return occurrence;
    }, [expression, databaseName, taskId, isFullBackup]);

    const nextOccurrence = asyncNextOccurrence.result;
    const errorMessage = validationError ?? (nextOccurrence?.IsValid === false ? nextOccurrence.ErrorMessage : null);

    if (errorMessage) {
        return (
            <div className="panel-bg-3 rounded-2 p-2 text-center text-danger small">
                <Icon icon="warning" />
                {errorMessage}
            </div>
        );
    }

    return (
        <div className="panel-bg-3 rounded-2 p-2 text-center">
            <div>
                <Icon icon="calendar" />
                {description} <span className="text-muted">{t("serverTimeNote")}</span>
            </div>
            <div>
                {t("nextRun")}{" "}
                {asyncNextOccurrence.loading ? <Spinner size="sm" /> : <NextOccurrence dto={nextOccurrence} />}
            </div>
        </div>
    );
}

function NextOccurrence({ dto }: { dto: Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence }) {
    const t = useStudioTranslation("cronEditor");

    if (!dto?.IsValid) {
        return <>{t("nextRunNotAvailable")}</>;
    }

    const nextOccurrenceUtc = moment.utc(dto.Utc);
    const localTime = genUtils.formatUtcDateAsLocal(dto.Utc);
    const serverTime = moment(dto.ServerTime).format(genUtils.dateFormat);
    const interval = nextOccurrenceUtc.isBefore(moment.utc())
        ? t("runOnSave")
        : genUtils.formatDurationByDate(nextOccurrenceUtc, true);

    return (
        <span title={serverTime !== localTime ? t("serverTime", { time: serverTime }) : undefined}>
            {localTime} ({interval})
        </span>
    );
}

const SYNTAX_CHART = `+----------------> minute (0 - 59) (, - * /)
|  +-------------> hour (0 - 23) (, - * /)
|  |  +----------> day of month (1 - 31) (, - * ? / L W)
|  |  |  +-------> month (1-12 or JAN-DEC) (, - * /)
|  |  |  |  +----> day of week (0-6 or SUN-SAT) (, - * ? / L #)
|  |  |  |  |
*  *  *  *  *`;
