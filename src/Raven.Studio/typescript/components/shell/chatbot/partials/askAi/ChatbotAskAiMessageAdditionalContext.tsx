import { yupResolver } from "@hookform/resolvers/yup";
import {
    RunChatbotAiAssistantResultDto,
    AdditionalContextOption,
} from "commands/aiAssistant/runChatbotAiAssistantCommand";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormSelectAutocomplete, FormInput } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DatabaseOptionItem from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/DatabaseOptionItem";
import DatabaseSingleValue from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/DatabaseSingleValue";
import NoDatabasePlaceholder from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/NoDatabasePlaceholder";
import { DatabaseSwitcherOption } from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/databaseSwitcherTypes";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import { useMemo } from "react";
import { useForm, SubmitHandler, useFieldArray, FieldArrayWithId, Control, FieldPath } from "react-hook-form";
import { ChatbotUserActionState, chatbotActions } from "../../store/chatbotSlice";
import Form from "react-bootstrap/Form";
import { FormGroup, FormLabel, FormSelect } from "components/common/Form";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";
import * as yup from "yup";

interface ChatbotAskAiMessageAdditionalContextProps {
    id: string;
    additionalContext: RunChatbotAiAssistantResultDto["AdditionalContext"];
    userActionState: ChatbotUserActionState;
}

export default function ChatbotAskAiMessageAdditionalContext({
    id,
    additionalContext,
    userActionState,
}: ChatbotAskAiMessageAdditionalContextProps) {
    const dispatch = useAppDispatch();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { control, handleSubmit, formState } = useForm<ActionContextFormData>({
        defaultValues: {
            items: Object.entries(additionalContext).map(([toolId, context]) => ({
                toolId,
                option: context.Option,
                label: context.Message,
                value: context.Option === "DatabaseName" ? activeDatabaseName : "",
            })),
        },
        resolver: actionContextResolver,
    });

    const itemsFieldsArray = useFieldArray({
        control,
        name: "items",
    });

    const handleSend: SubmitHandler<ActionContextFormData> = (data) => {
        return tryHandleSubmit(async () => {
            const actionResponses = Object.fromEntries(data.items.map((item) => [item.toolId, item.value]));
            dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "allowed" } }));
            dispatch(chatbotActions.runChat({ actionResponses }));
        });
    };

    const handleSkip = () => {
        const actionResponses = Object.fromEntries(Object.keys(additionalContext).map((toolId) => [toolId, "Skipped"]));
        dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "skipped" } }));
        dispatch(chatbotActions.runChat({ actionResponses }));
    };

    return (
        <Form className="well border border-secondary rounded-2" onSubmit={handleSubmit(handleSend)}>
            <div className="fs-6 py-1 px-2 border-bottom border-secondary">
                <Icon icon="about" />
                Additional context
            </div>
            <div className="p-2">
                {itemsFieldsArray.fields.map((field, index) => (
                    <FormGroup key={field.id}>
                        <FormLabel>{field.label}</FormLabel>
                        <FormOptionField control={control} field={field} index={index} />
                    </FormGroup>
                ))}
                <div className="hstack justify-content-end mt-2">
                    {userActionState === "waiting" && (
                        <div className="hstack gap-1">
                            <Button variant="link" className="text-emphasis" size="xs" onClick={handleSkip}>
                                Skip
                            </Button>
                            <ButtonWithSpinner
                                variant="primary"
                                type="submit"
                                isSpinning={formState.isSubmitting}
                                className="rounded-pill"
                                size="sm"
                            >
                                Send
                            </ButtonWithSpinner>
                        </div>
                    )}
                    {userActionState === "skipped" && (
                        <Badge bg="secondary" className="rounded-pill">
                            <Icon icon="skip" />
                            Skipped
                        </Badge>
                    )}
                    {userActionState === "allowed" && (
                        <Badge bg="success" className="rounded-pill">
                            <Icon icon="check" />
                            Success
                        </Badge>
                    )}
                </div>
            </div>
        </Form>
    );
}

interface FormOptionFieldProps {
    control: Control<ActionContextFormData>;
    field: FieldArrayWithId<ActionContextFormData, "items", "id">;
    index: number;
}

function FormOptionField({ control, field, index }: FormOptionFieldProps) {
    const name = `items.${index}.value` as const;

    if (field.option === "DatabaseName") {
        return <FormDatabaseField control={control} name={name} />;
    }

    if (field.option === "CollectionName") {
        return <FormCollectionField control={control} name={name} />;
    }

    return <FormInput type="text" control={control} name={name} className="rounded-pill" />;
}

interface FormFieldProps {
    control: Control<ActionContextFormData>;
    name: FieldPath<ActionContextFormData>;
}

function FormDatabaseField({ control, name }: FormFieldProps) {
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);

    const databaseOptions: DatabaseSwitcherOption[] = useMemo(() => {
        const sortedByNameDatabases = allDatabases.sort((a, b) => a.name.localeCompare(b.name));
        const sortedByStatusDatabases = [
            ...sortedByNameDatabases.filter((item) => !item.isDisabled),
            ...sortedByNameDatabases.filter((item) => item.isDisabled),
        ];

        return sortedByStatusDatabases.map((db) => ({
            value: db.name,
            isSharded: db.isSharded,
            environment: db.environment,
            isDisabled: db.isDisabled,
        }));
    }, [allDatabases]);

    return (
        <FormSelect
            control={control}
            name={name}
            placeholder={<NoDatabasePlaceholder />}
            options={databaseOptions}
            components={{ Option: DatabaseOptionItem, SingleValue: DatabaseSingleValue }}
            isRoundedPill
        />
    );
}

function FormCollectionField({ control, name }: FormFieldProps) {
    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    return <FormSelectAutocomplete control={control} name={name} options={collectionOptions} isRoundedPill />;
}

const actionContextSchema = yup.object({
    items: yup.array().of(
        yup.object({
            toolId: yup.string(),
            option: yup.string<AdditionalContextOption>(),
            label: yup.string(),
            value: yup.string().nullable().required(),
        })
    ),
});

const actionContextResolver = yupResolver(actionContextSchema);
type ActionContextFormData = yup.InferType<typeof actionContextSchema>;
