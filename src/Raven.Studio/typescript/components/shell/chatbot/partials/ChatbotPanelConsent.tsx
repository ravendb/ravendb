import IconAsciiPlaceholder from "components/shell/chatbot/partials/askAi/iconAscii/IconAsciiPlaceholder";
import Button from "react-bootstrap/Button";

export default function ChatbotPanelConsent() {
    return (
        <div>
            <IconAsciiPlaceholder />
            <h3 className="mt-4 mb-0 fw-semibold">AI Assistant</h3>
            <div className="mt-2">
                To use our built-in AI features, such as <i>AI Assistant</i>, you need to provide consent.
                <br />
                The feature will remain unavailable until accepted.
                <div className="mt-3">
                    <Button variant="primary" className="rounded-pill">
                        Review the consent
                    </Button>
                </div>
            </div>
        </div>
    );
}
