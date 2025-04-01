import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function ClusterDebugAboutView() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>This view shows the Raft command log state and entries for each node in the cluster.</p>

                <p>
                    <strong>Summary</strong>:<br />
                    Displays statistics related to the Raft command log for each node, as well as the node&apos;s
                    connection state to other nodes in the cluster.
                    <br />
                    <br />
                    <strong>Log entries</strong>:<br />
                    Lists all log entries for each node. Each entry represents a Raft command that was appended to the
                    log, along with its status. A size of 0 indicates that the command was committed on the node.
                </p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
