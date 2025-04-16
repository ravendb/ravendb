import "./ProtractedRequestMessage.scss";
import protractedCommandsDetector from "common/notifications/protractedCommandsDetector";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";

export default function ProtractedRequestMessage() {
    return (
        <div className="protracted-request-message hidden">
            <Spinner size="sm" variant="progress" className="me-2" />
            <span>Making things happen... Please hold on.</span>
            <Button
                variant="link"
                size="sm"
                className="text-reset ms-4"
                onClick={() => protractedCommandsDetector.instance.clearRequests()}
            >
                <Icon icon="close" margin="m-0" />
            </Button>
        </div>
    );
}
