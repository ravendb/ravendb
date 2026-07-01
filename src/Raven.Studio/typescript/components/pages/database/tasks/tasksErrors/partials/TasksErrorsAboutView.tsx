import React from "react";
import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";

export default function TasksErrorsAboutView() {
    const taskErrorsOverviewDocsLink = useRavenLink({ hash: "17261M" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info">
                <p>
                    This view displays recent errors from your ongoing tasks, including <strong>ETL tasks</strong>{" "}
                    (RavenDB, SQL, OLAP, ElasticSearch, Kafka, RabbitMQ, Azure Queue Storage, Amazon SQS, Snowflake) and{" "}
                    <strong>AI tasks</strong> (Embeddings Generation, GenAI).
                </p>
                <h4>Error types</h4>
                <ul>
                    <li>
                        <strong>Item error</strong> - An error that occurred while processing a single document. The
                        document was skipped, and the task continued processing the remaining documents.
                    </li>
                    <li>
                        <strong>Process error</strong> - An error that occurred while processing a batch and may affect
                        multiple documents.
                    </li>
                </ul>
                <h4>Error steps</h4>
                <ul>
                    <li>
                        <strong>Transformation</strong> - Error during the transformation script execution.
                    </li>
                    <li>
                        <strong>Load</strong> - Error while loading data to the destination.
                    </li>
                    <li>
                        <strong>Model Inference</strong> - Error during communication with the AI model (AI tasks only).
                    </li>
                    <li>
                        <strong>Persistence</strong> - Error while saving results back to the database (AI tasks only).
                    </li>
                    <li>
                        <strong>Configuration</strong> - Error related to task configuration.
                    </li>
                </ul>
                <h4>Task health</h4>
                <ul>
                    <li className="mb-2">
                        Each task has a health status based on its recent error rate
                        <br />
                        (calculated using an exponentially weighted moving average):
                        <ul>
                            <li>
                                <strong>Healthy</strong> - No errors, or only a low error rate.
                            </li>
                            <li>
                                <strong>Impaired</strong> - An increased error rate that needs attention.
                            </li>
                            <li>
                                <strong>Failed</strong> - A high error rate that needs immediate attention.
                            </li>
                        </ul>
                    </li>
                    <li>The health status recovers automatically as new batches complete successfully.</li>
                </ul>
                <h4>Error storage</h4>
                <ul>
                    <li className="mb-2">
                        Errors are persisted on disk. A maximum of 500 item errors and 500 process errors are stored per
                        task. Older errors are automatically removed when this limit is reached.
                    </li>
                    <li>
                        You can manually delete the current task errors.
                        <br />
                        However, deleting the errors will not reset a task in an Error state back to the Normal state.
                        The task health will recover gradually as new batches complete successfully.
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={taskErrorsOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Task Errors Overview
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
