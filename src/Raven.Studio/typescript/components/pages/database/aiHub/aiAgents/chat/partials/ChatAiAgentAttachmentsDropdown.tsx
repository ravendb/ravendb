import { CustomDropdownToggle } from "components/common/Dropdown";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { useServices } from "components/hooks/useServices";
import {
    chatAiAgentActions,
    chatAiAgentSelectors,
} from "components/pages/database/aiHub/aiAgents/chat/store/chatAiAgentSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { ChangeEvent, useMemo, useRef, useState } from "react";
import { useAsync } from "react-async-hook";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import document from "models/database/documents/document";
import { LoadError } from "components/common/LoadError";
import { SubmitHandler, UseFieldArrayReturn, useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { FormCheckboxes, FormCheckboxesOption } from "components/common/Form";
import InnerForm from "components/common/InnerForm";
import { ChatAiAgentFormData } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentValidation";
import { chatAiAgentAttachmentsUtils } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentAttachmentsUtils";

interface ChatAiAgentAttachmentDropdownProps {
    attachmentsFieldsArray: UseFieldArrayReturn<ChatAiAgentFormData, "attachments", "id">;
    isPromptDisabled: boolean;
}

export default function ChatAiAgentAttachmentsDropdown({
    attachmentsFieldsArray,
    isPromptDisabled,
}: ChatAiAgentAttachmentDropdownProps) {
    const dispatch = useAppDispatch();
    const tabInfo = useAppSelector(chatAiAgentSelectors.newAttachmentTab);

    return (
        <Dropdown
            show={tabInfo != null}
            onToggle={() =>
                dispatch(chatAiAgentActions.newAttachmentTabSet(tabInfo != null ? null : { tab: "source" }))
            }
            autoClose="outside"
            className="me-auto"
        >
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                isCaretHidden
                variant="link"
                title="Add attachments"
                className="p-0"
                disabled={isPromptDisabled}
            >
                <Icon icon="attachment" margin="m-0" />
            </Dropdown.Toggle>
            <Dropdown.Menu
                style={{ width: 400, height: tabInfo?.tab === "source" ? undefined : 350 }}
                renderOnMount
                popperConfig={{ strategy: "fixed" }}
            >
                <div className="vstack h-100">
                    {tabInfo?.tab === "source" && <SourceTab attachmentsFieldsArray={attachmentsFieldsArray} />}
                    {tabInfo?.tab === "document" && <DocumentTab />}
                    {tabInfo?.tab === "documentAttachments" && (
                        <DocumentAttachmentsTab chatAttachmentsFieldsArray={attachmentsFieldsArray} />
                    )}
                </div>
            </Dropdown.Menu>
        </Dropdown>
    );
}

function SourceTab({ attachmentsFieldsArray }: Pick<ChatAiAgentAttachmentDropdownProps, "attachmentsFieldsArray">) {
    const dispatch = useAppDispatch();
    const fileInputRef = useRef<HTMLInputElement>(null);
    const conversationDocument = useAppSelector(chatAiAgentSelectors.document);

    const handleLocalFilesChange = (event: ChangeEvent<HTMLInputElement>) => {
        const files = Array.from(event.target.files || []);

        if (!files.length) {
            event.target.value = "";
            return;
        }

        const { attachments, invalidFiles } = chatAiAgentAttachmentsUtils.prepareConversationLocalFiles(
            files,
            attachmentsFieldsArray.fields.map((x) => x.name),
            conversationDocument.data?.["@metadata"]?.["@attachments"]?.map((a) => a.Name) ?? []
        );

        if (attachments.length) {
            attachmentsFieldsArray.append(attachments);
            dispatch(chatAiAgentActions.newAttachmentTabSet(null));
        }

        chatAiAgentAttachmentsUtils.reportValidationErrors(invalidFiles);
        event.target.value = "";
    };

    return (
        <>
            <input
                ref={fileInputRef}
                type="file"
                multiple
                accept={chatAiAgentAttachmentsUtils.validExtensionsAccept}
                className="d-none"
                onChange={handleLocalFilesChange}
            />
            <Dropdown.Header className="hstack justify-content-between">
                <div className="small-label">Choose location</div>
            </Dropdown.Header>
            <Dropdown.Item onClick={() => fileInputRef.current?.click()}>
                <Icon icon="file-import" />
                Local files
            </Dropdown.Item>
            <Dropdown.Item onClick={() => dispatch(chatAiAgentActions.newAttachmentTabSet({ tab: "document" }))}>
                <Icon icon="document" />
                Document attachments
            </Dropdown.Item>
        </>
    );
}

function DocumentTab() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const conversationId = useAppSelector(chatAiAgentSelectors.conversationId);
    const [idPrefix, setIdPrefix] = useState("");
    const { databasesService } = useServices();

    const asyncGetDocumentIds = useAsyncDebounce(
        async () => {
            if (!databaseName) {
                return [];
            }

            try {
                if (!idPrefix) {
                    const lastModifiedDocs = await databasesService.getDocumentsPreview(databaseName, 0, 10, undefined);
                    return lastModifiedDocs.items.map((x) => x.getId()).filter((id) => id !== conversationId);
                }

                const results = await databasesService.getDocumentsMetadataByIDPrefix(idPrefix, 10, databaseName);
                return results.map((x) => x["@metadata"]["@id"]).filter((id) => id !== conversationId);
            } catch {
                return [];
            }
        },
        [idPrefix, databaseName],
        300
    );

    const handleSelect = (documentId: string) => {
        dispatch(chatAiAgentActions.newAttachmentTabSet({ tab: "documentAttachments", documentId }));
    };

    return (
        <>
            <Dropdown.Header>
                <span className="small-label">
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 text-emphasis"
                        onClick={() => dispatch(chatAiAgentActions.newAttachmentTabSet({ tab: "source" }))}
                    >
                        <Icon icon="arrow-left" margin="me-1" />
                    </Button>
                    Document ID
                </span>
            </Dropdown.Header>

            <div className="clearable-input mb-2">
                <Form.Control
                    type="text"
                    value={idPrefix}
                    onChange={(e) => setIdPrefix(e.target.value)}
                    placeholder="Enter document ID prefix"
                    className="rounded-pill ps-3 pe-4"
                />
                {idPrefix && (
                    <div className="clear-button">
                        <Button variant="secondary" size="sm" onClick={() => setIdPrefix("")}>
                            <Icon icon="clear" margin="m-0" />
                        </Button>
                    </div>
                )}
            </div>
            {asyncGetDocumentIds.loading && <ListSkeleton />}
            {asyncGetDocumentIds.result?.length === 0 && !asyncGetDocumentIds.loading && (
                <EmptySet compact className="hstack justify-content-center mt-3">
                    No documents found
                </EmptySet>
            )}
            <div className="overflow-y-auto flex-grow-1">
                {asyncGetDocumentIds.result?.length > 0 &&
                    asyncGetDocumentIds.result.map((documentId) => (
                        <Dropdown.Item
                            key={documentId}
                            onClick={() => handleSelect(documentId)}
                            className="text-truncate"
                            title={documentId}
                        >
                            {documentId}
                        </Dropdown.Item>
                    ))}
            </div>
        </>
    );
}

interface DocumentAttachmentsTabProps {
    chatAttachmentsFieldsArray: UseFieldArrayReturn<ChatAiAgentFormData, "attachments", "id">;
}

function DocumentAttachmentsTab({ chatAttachmentsFieldsArray }: DocumentAttachmentsTabProps) {
    const dispatch = useAppDispatch();
    const tabInfo = useAppSelector(chatAiAgentSelectors.newAttachmentTab);
    const { databasesService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const conversationDocAttachments = useAppSelector(chatAiAgentSelectors.documentAttachments);

    const documentId = tabInfo?.tab === "documentAttachments" ? tabInfo.documentId : null;

    const asyncDocAttachments = useAsync(async (): Promise<documentAttachmentDto[]> => {
        const result = await databasesService.getDocumentWithMetadata(documentId, databaseName);
        if (result instanceof document) {
            return result.__metadata.attachments() ?? [];
        }

        throw new Error("Document not found");
    }, [documentId]);

    const hasAttachmentsInDoc = asyncDocAttachments.result?.length > 0;
    const hasNoAttachmentsInDoc = asyncDocAttachments.result?.length === 0;

    const attachmentOptions: FormCheckboxesOption[] = useMemo(() => {
        if (!hasAttachmentsInDoc) {
            return [];
        }

        return (
            asyncDocAttachments.result?.map((attachment) => ({
                value: attachment.Name,
                label: attachment.Name,
            })) ?? []
        );
    }, [asyncDocAttachments.result, hasAttachmentsInDoc]);

    const { control, handleSubmit } = useForm<DocumentAttachmentsFormData>({
        defaultValues: {
            names: [],
        },
        resolver: yupResolver(documentAttachmentsSchema),
    });

    const selectedNames = useWatch({
        control,
        name: "names",
    });

    const handleAdd: SubmitHandler<DocumentAttachmentsFormData> = (data) => {
        const reservedNames = chatAiAgentAttachmentsUtils.createReservedNames([
            ...chatAttachmentsFieldsArray.fields.map((x) => x.name),
            ...conversationDocAttachments.map((attachment) => attachment.Name),
        ]);

        chatAttachmentsFieldsArray.append(
            data.names.map((name) => ({
                type: "documentAttachment",
                name: chatAiAgentAttachmentsUtils.createUniqueName(name, reservedNames),
                originalName: name,
                contentType: asyncDocAttachments.result?.find((attachment) => attachment.Name === name)?.ContentType,
                sourceDocumentId: documentId,
            }))
        );
        dispatch(chatAiAgentActions.newAttachmentTabSet(null));
    };

    return (
        <InnerForm onSubmit={handleSubmit(handleAdd)} className="h-100 vstack">
            <Dropdown.Header>
                <div className="hstack">
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 text-emphasis"
                        onClick={() => dispatch(chatAiAgentActions.newAttachmentTabSet({ tab: "document" }))}
                    >
                        <Icon icon="arrow-left" margin="me-1" />
                    </Button>
                    <span className="small-label">Document attachments</span>
                    {hasAttachmentsInDoc && (
                        <Button
                            variant="primary"
                            size="sm"
                            onClick={handleSubmit(handleAdd)}
                            className="ms-auto"
                            disabled={selectedNames.length === 0}
                        >
                            <Icon icon="plus" />
                            Add to prompt
                        </Button>
                    )}
                </div>
            </Dropdown.Header>
            <div className="overflow-y-auto flex-grow-1">
                {asyncDocAttachments.loading && <ListSkeleton />}
                {asyncDocAttachments.error && <LoadError error="Failed to load attachments" />}
                {hasAttachmentsInDoc && (
                    <FormCheckboxes
                        control={control}
                        name="names"
                        options={attachmentOptions}
                        className="px-2 pb-2 gap-1"
                    />
                )}
                {hasNoAttachmentsInDoc && <EmptySet compact>No attachments found</EmptySet>}
            </div>
        </InnerForm>
    );
}

const documentAttachmentsSchema = yup.object({
    names: yup.array().of(yup.string()).min(1, "Select at least one attachment"),
});

type DocumentAttachmentsFormData = yup.InferType<typeof documentAttachmentsSchema>;

function ListSkeleton() {
    return (
        <LazyLoad active className="vstack gap-1">
            <div style={{ height: 35 }} />
            <div style={{ height: 35 }} />
            <div style={{ height: 35 }} />
        </LazyLoad>
    );
}
