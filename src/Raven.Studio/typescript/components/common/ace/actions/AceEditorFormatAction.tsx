import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAceEditorContext } from "../AceEditorContext";
import "ace-builds/src-noconflict/ext-beautify";

const beautify = ace.require("ace/ext/beautify").beautify;

export default function AceEditorFormatAction() {
    const reactAce = useAceEditorContext();

    return (
        <Button
            variant="link"
            onClick={() => beautify(reactAce?.current.editor.session)}
            className="p-0 text-reset"
            size="sm"
            title="Format"
        >
            <Icon icon="indent" margin="m-0" />
        </Button>
    );
}
