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
import { useForm, SubmitHandler } from "react-hook-form";
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
    const allAdditionalContextOptions = Object.values(additionalContext).map((option) => option.Option);

    const isOption = (option: AdditionalContextOption) => allAdditionalContextOptions.includes(option);

    const getToolCallId = (option: AdditionalContextOption) => {
        return Object.keys(additionalContext).find((key) => additionalContext[key].Option === option);
    };

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
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

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    const { control, handleSubmit, formState } = useForm<ActionContextFormData>({
        defaultValues: {
            databaseName:
                isOption("DatabaseName") && activeDatabaseName
                    ? (databaseOptions?.find((x) => x.value === activeDatabaseName)?.value ?? null)
                    : null,
            collectionName: null,
            documentId: null,
            indexName: null,
        },
        resolver: yupResolver(actionContextSchema),
    });

    const handleSend: SubmitHandler<ActionContextFormData> = (data) => {
        return tryHandleSubmit(async () => {
            const actionResponses: Record<string, any> = {};

            if (isOption("DatabaseName")) {
                actionResponses[getToolCallId("DatabaseName")] = data.databaseName;
            }
            if (isOption("CollectionName")) {
                actionResponses[getToolCallId("CollectionName")] = data.collectionName;
            }
            if (isOption("DocumentId")) {
                actionResponses[getToolCallId("DocumentId")] = data.documentId;
            }
            if (isOption("IndexName")) {
                actionResponses[getToolCallId("IndexName")] = data.indexName;
            }

            dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "allowed" } }));
            dispatch(chatbotActions.runChat({ actionResponses }));
        });
    };

    const handleSkip = () => {
        const actionResponses: Record<string, any> = {};

        for (const option of allAdditionalContextOptions) {
            actionResponses[getToolCallId(option)] = "Skipped";
        }

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
                {isOption("DatabaseName") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Database</FormLabel>
                            <FormSelect
                                control={control}
                                name="databaseName"
                                placeholder={<NoDatabasePlaceholder />}
                                options={databaseOptions}
                                components={{ Option: DatabaseOptionItem, SingleValue: DatabaseSingleValue }}
                                isRoundedPill
                            />
                        </FormGroup>
                    </div>
                )}
                {isOption("CollectionName") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Collection</FormLabel>
                            <FormSelectAutocomplete
                                control={control}
                                name="collectionName"
                                options={collectionOptions}
                                isRoundedPill
                            />
                        </FormGroup>
                    </div>
                )}
                {isOption("DocumentId") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Document ID</FormLabel>
                            <FormInput type="text" control={control} name="documentId" className="rounded-pill" />
                        </FormGroup>
                    </div>
                )}
                {isOption("IndexName") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Index name</FormLabel>
                            <FormInput type="text" control={control} name="indexName" className="rounded-pill" />
                        </FormGroup>
                    </div>
                )}
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

const actionContextSchema = yup.object({
    databaseName: yup.string().nullable(),
    collectionName: yup.string().nullable(),
    documentId: yup.string().nullable(),
    indexName: yup.string().nullable(),
});

type ActionContextFormData = yup.InferType<typeof actionContextSchema>;
