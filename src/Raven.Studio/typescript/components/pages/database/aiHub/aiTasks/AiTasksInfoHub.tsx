import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function AiTasksInfoHub() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
                targetId="about-view"
            >
                <p>
                    In this view, you can manage AI tasks that generate embeddings - create new tasks, edit existing
                    ones,
                    <br />
                    or delete them as needed.
                </p>
                <div>
                    These AI tasks:
                    <ul>
                        <li>Extract text from your documents,</li>
                        <li>Connect to an AI service to generate embeddings from that text,</li>
                        <li>Save the generated embeddings in a dedicated collection in the database.</li>
                    </ul>
                </div>
                <p>
                    The generated embeddings can then be used to perform <strong>vector search queries</strong>.
                </p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
