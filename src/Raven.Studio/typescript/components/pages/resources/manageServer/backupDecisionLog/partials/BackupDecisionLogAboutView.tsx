import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function BackupDecisionLogAboutView() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    The server backup runner keeps every periodic backup task of every database in a single queue and
                    evaluates it on each tick. This view exposes what the runner decided and why.
                </p>

                <p>
                    <strong>Queue</strong>:<br />
                    Live state of the runner on this node — how many tasks are waiting in the queue, how many backups
                    are currently running against the concurrency limit, and which task is scheduled next.
                    <br />
                    <br />
                    <strong>Decisions</strong>:<br />
                    Each entry is one decision, newest first. <code>Policy</code> entries mean a backup was not started
                    because a server-level or task-level policy blocked it (not yet time, disabled, another backup
                    running, low memory, and so on). <code>Started</code>, <code>Completed</code>, <code>Failed</code>{" "}
                    and <code>Cancelled</code> entries track the lifecycle of backups that did run.
                </p>

                <p>
                    Decisions are kept in memory only, per node, and the oldest ones are dropped once the per-task limit
                    is reached. Restarting the server clears them.
                </p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
