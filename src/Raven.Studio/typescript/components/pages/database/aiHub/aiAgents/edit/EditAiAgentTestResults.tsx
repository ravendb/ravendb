import { Icon } from "components/common/Icon";

export default function EditAiAgentTestResults() {
    return (
        <>
            <div className="panel-bg-2 p-3 border-bottom border-secondary">
                <h3 className="m-0">
                    <Icon icon="test" color="primary" />
                    Test results
                </h3>
            </div>
            <div className="p-3 flex-grow-1 vstack justify-content-center align-items-center">
                <Icon icon="test" color="primary" className="fs-1" />
                <p className="mt-2 text-center">
                    This is a testing environment for your AI Agent. Once everything is configured, click the “Test”
                    button to see the results.
                </p>
            </div>
        </>
    );
}
