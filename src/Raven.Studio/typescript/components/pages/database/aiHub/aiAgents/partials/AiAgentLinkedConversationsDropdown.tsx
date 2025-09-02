import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import Dropdown from "react-bootstrap/Dropdown";

interface AiAgentLinkedConversationsDropdownProps {
    linkedConversations: string[];
}

export default function AiAgentLinkedConversationsDropdown({
    linkedConversations,
}: AiAgentLinkedConversationsDropdownProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();

    const existingLinkedConversations = linkedConversations.filter(Boolean);

    if (existingLinkedConversations.length === 0) {
        return null;
    }

    return (
        <Dropdown>
            <Dropdown.Toggle
                title="Linked conversations"
                variant="outline-info"
                className="rounded-pill"
                as={CustomDropdownToggle}
            >
                <Icon icon="documents" />
                Linked conversations
            </Dropdown.Toggle>
            <Dropdown.Menu
                style={{ maxWidth: "500px", maxHeight: "320px" }}
                className="panel-bg-1 p-3 rounded-2 overflow-auto"
            >
                {existingLinkedConversations.filter(Boolean).map((docId) => (
                    <Dropdown.Item
                        href={appUrl.forEditDoc(docId, databaseName)}
                        target="_blank"
                        title={docId}
                        className="text-truncate"
                    >
                        <span title={docId}>{getShortDocumentId(docId)}</span>
                        <Icon icon="newtab" margin="ms-1" />
                    </Dropdown.Item>
                ))}
            </Dropdown.Menu>
        </Dropdown>
    );
}

function getShortDocumentId(docId: string) {
    return docId.split("/")[1].split("$")[0];
}
