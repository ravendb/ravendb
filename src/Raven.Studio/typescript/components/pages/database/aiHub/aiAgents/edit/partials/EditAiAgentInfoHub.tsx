import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function EditAiAgentInfoHub() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
                targetId="about-view"
            >
                <div>
                    In this view, you can define an AI Agent - <br /> a natural language{" "}
                    <strong>conversational assistant</strong> powered by an LLM.
                    <br />
                    <br />
                    The agent lets you chat with an LLM about your data. It can retrieve information from your database
                    to answer prompts, and trigger specific actions when needed.
                    <br />
                    <br />
                    The configuration includes:
                    <ul className="mt-1">
                        <li>
                            <strong>Connection string</strong> - Specifies the AI provider and the LLM of your choice.
                        </li>
                        <li className="mt-1">
                            <strong>Response structure</strong> - Define the expected format of the agent’s replies.
                        </li>
                        <li className="mt-1">
                            <strong>Chat storage</strong> - Configure the documents that store chat conversations.
                            <br /> You can optionally enable trimming or summarization of their content.
                        </li>
                        <li className="mt-1">
                            <strong>Tools</strong> - Tools are a controlled way to pass context to the LLM:
                            <ul>
                                <li className="mt-1">
                                    <strong>Query tools</strong>: <br /> Define queries the agent can run against your
                                    database when the LLM thinks they’re needed to respond to the user. Scope access by
                                    setting agent parameters - ensuring the LLM receives only the data its allowed to
                                    access.
                                </li>
                                <li className="mt-1">
                                    <strong>Action tools</strong>: <br /> Define tasks the agent can trigger when
                                    instructed by the LLM in response to user prompts.
                                </li>
                            </ul>
                        </li>
                        <li className="mt-1">
                            <strong>Sub-agents</strong> - Assign other AI agents that this agent can delegate
                            specialized tasks to. The sub-agent runs its own tools (queries, actions) and returns its
                            response to the parent agent.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
