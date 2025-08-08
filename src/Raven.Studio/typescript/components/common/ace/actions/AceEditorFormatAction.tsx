import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAceEditorContext } from "../AceEditorContext";
import "ace-builds/src-noconflict/ext-beautify";

const beautify = ace.require("ace/ext/beautify").beautify;

export default function AceEditorFormatAction() {
    const reactAce = useAceEditorContext();

    const handleFormat = () => {
        try {
            const value = reactAce?.current.editor.session.getValue();
            const parsed = JSON.parse(value);
            const formatted = JSON.stringify(parsed, null, 2);
            reactAce?.current.editor.session.setValue(formatted);
        } catch {
            beautify(reactAce?.current.editor.session);
        }
    };

    return (
        <Button variant="link" onClick={handleFormat} className="p-0 text-reset" size="sm" title="Format">
            <Icon icon="indent" margin="m-0" />
        </Button>
    );
}
