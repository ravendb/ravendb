import { AboutViewHeading } from "components/common/AboutView";

export default function EditAiAgent() {
    return (
        <div className="content-padding vstack">
            <AboutViewHeading title="Create AI Agent" icon="ai-agents" marginBottom={4} />

            <div className="vstack flex-grow-1">
                <div className="hstack gap-2 flex-grow-1 align-items-start">
                    <div style={{ background: "red" }} className="flex-grow-1">
                        Main area
                    </div>
                    <div style={{ background: "blue" }}>Test area</div>
                </div>
                <div>Footer</div>
            </div>
        </div>
    );
}
