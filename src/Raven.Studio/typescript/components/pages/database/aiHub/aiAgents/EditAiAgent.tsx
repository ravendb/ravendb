import { AboutViewHeading } from "components/common/AboutView";
import useResizableWidth from "./hooks/useResizableWidth";
import { Icon } from "components/common/Icon";

export default function EditAiAgent() {
    const testAreaResizable = useResizableWidth({
        initialWidth: 500,
        minWidth: 500,
        maxWidth: 1000,
    });

    return (
        <div className="hstack h-100">
            <div className="vstack flex-grow-1" style={{ width: "min-content" }}>
                <div className="p-3 flex-grow-1 w-auto vstack">
                    <AboutViewHeading title="Create AI Agent" icon="ai-agents" marginBottom={4} />

                    <div className="flex-grow-1 overflow-auto">
                        <h3>Configure basic settings</h3>
                        <p>
                            Setup basic information about your agent - give it a specific task, database it will connect
                            to and format in which agent will respond.
                        </p>
                    </div>
                </div>
                <div className="p-3 border-top border-secondary">Footer</div>
            </div>
            <div
                style={{
                    width: `${testAreaResizable.width}px`,
                    position: "relative",
                    borderLeft: `1px solid ${testAreaResizable.isDragging ? "#ccc" : "#4c4c63"}`,
                }}
                className="panel-bg-1 h-100 vstack"
            >
                <div
                    style={{
                        position: "absolute",
                        left: "-5px",
                        top: 0,
                        bottom: 0,
                        width: "10px",
                        cursor: "col-resize",
                    }}
                    onMouseDown={testAreaResizable.handleMouseDown}
                />
                <div className="panel-bg-2 p-3 border-bottom border-secondary">
                    <h3 className="m-0">
                        <Icon icon="test" color="primary" />
                        Test results
                    </h3>
                </div>
                <div className="p-3 flex-grow-1 vstack justify-content-center align-items-center">
                    <Icon icon="test" color="primary" className="fs-1" />
                    <p className="mt-2">
                        This is a testing environment for your AI Agent. Once everything is configured, click the “Test”
                        button to see the results.
                    </p>
                </div>
            </div>
        </div>
    );
}
