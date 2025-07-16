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
                TODO
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
