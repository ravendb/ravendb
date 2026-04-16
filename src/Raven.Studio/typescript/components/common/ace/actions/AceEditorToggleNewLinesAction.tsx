import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAceEditorContext } from "../AceEditorContext";
import useBoolean from "components/hooks/useBoolean";
import messagePublisher from "common/messagePublisher";
import documentHelpers from "common/helpers/database/documentHelpers";
import { handleFormat } from "./AceEditorFormatAction";
import classNames from "classnames";

(ace as any).config.loadModule("ace/mode/raven_document_newline_friendly");

export default function AceEditorToggleNewLinesAction() {
    const { aceRef } = useAceEditorContext();
    const { value: isNewLinesEnabled, toggle: toggleNewLinesEnabled } = useBoolean(false);

    const handleToggleNewLines = () => {
        try {
            const session = aceRef?.current.editor.session;

            if (!session) {
                console.error("No Ace Editor session found");
                return;
            }

            if (isNewLinesEnabled) {
                const value = documentHelpers.escapeNewlinesAndTabsInTextFields(session.getValue());
                session.setValue(value);
                session.setMode("ace/mode/raven_document");
                handleFormat(aceRef);
            } else {
                const value = documentHelpers.unescapeNewlinesAndTabsInTextFields(session.getValue());
                session.setValue(value);
                session.setMode("ace/mode/raven_document_newline_friendly");
            }

            toggleNewLinesEnabled();
        } catch (e) {
            console.error(e);
            messagePublisher.reportError("The document data isn't a legal JSON expression!");
        }
    };

    return (
        <Button
            variant="link"
            onClick={handleToggleNewLines}
            className={classNames("p-0 text-reset", { border: isNewLinesEnabled })}
            size="sm"
            title="Toggle new lines"
        >
            <Icon icon="newline" margin="m-0" />
        </Button>
    );
}
