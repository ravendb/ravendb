import IconAsciiPlaceholder from "components/shell/chatbot/partials/askAi/iconAscii/IconAsciiPlaceholder";
import Button from "react-bootstrap/Button";

export default function ChatbotPanelConsent() {
    return (
        <div>
            <IconAsciiPlaceholder />
            <h3 className="mt-4 mb-0 fw-semibold">AI Assistant</h3>
            <div className="mt-2">
                The built-in AI Assistant is designed to supercharge your workflow. To enable this feature, please
                review and accept the Terms of Use.
                <br />
                The feature will remain unavailable until accepted.
                <div className="mt-3">
                    <Button variant="primary" className="rounded-pill">
                        Review Terms of Use
                    </Button>
                </div>
            </div>
        </div>
    );
}
