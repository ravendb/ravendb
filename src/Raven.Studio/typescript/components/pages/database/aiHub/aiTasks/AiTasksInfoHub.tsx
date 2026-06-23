import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function AiTasksInfoHub() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <p>
                    In this view, you can manage AI tasks - <br />
                    create new tasks, edit existing ones, or delete them as needed.
                </p>
                <div>
                    <strong>GenAI tasks</strong>:
                    <ul>
                        <li>Use large language models (LLMs) to analyze and enrich your documents.</li>
                        <li>Automatically update documents or create new ones based on AI-generated content.</li>
                    </ul>
                </div>
                <div>
                    <strong>Embeddings generation tasks</strong>:
                    <ul>
                        <li>Extract text from your documents,</li>
                        <li>Connect to an AI service to generate embeddings from that text,</li>
                        <li>Save the generated embeddings in dedicated collections in the database.</li>
                        <li>Use the embeddings to perform vector search queries.</li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
