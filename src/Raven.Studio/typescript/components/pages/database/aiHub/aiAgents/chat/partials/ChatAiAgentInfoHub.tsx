import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function ChatAiAgentInfoHub() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <ul>
                        <li>
                            This chat view lets you interact with the agent by sending user prompts that are forwarded
                            to the LLM and receiving its responses.
                        </li>
                        <li className="mt-1">
                            When appropriate, the LLM can instruct the agent to invoke predefined tools - either to run
                            queries and retrieve data from the database, or to perform specific actions.
                        </li>
                        <li className="mt-1">
                            You can view the full interaction between the agent and the LLM, including tool calls
                            triggered during the conversation. Switch between the chat display and raw data format to
                            inspect how each user request is handled.
                        </li>
                        <li className="mt-1">
                            The chat session is saved in a dedicated document in the <code>@conversations</code>{" "}
                            collection.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
