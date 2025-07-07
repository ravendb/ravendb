import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAceEditorContext } from "../AceEditorContext";

export default function AceEditorFullScreenAction() {
    const reactAce = useAceEditorContext();

    return (
        <Button
            variant="link"
            onClick={() => {
                reactAce?.current.editor.container.requestFullscreen();
            }}
            className="p-0 text-reset"
            size="sm"
            title="Full screen"
        >
            <Icon icon="fullscreen" margin="m-0" />
        </Button>
    );
}
