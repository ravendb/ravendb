import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { AboutViewAnchored } from "components/common/AboutView";
import { useRavenLink } from "components/hooks/useRavenLink";

export default function CompareExchangeInfoHub() {
    const docsLink = useRavenLink({ hash: "2BGJN2" });

    // TODO
    return (
        <AboutViewFloating>
            <AboutViewAnchored className="my-4">
                <AccordionItemWrapper
                    targetId="1"
                    icon="about"
                    color="info"
                    description="Get additional info on this feature"
                    heading="About this view"
                >
                    TODO
                </AccordionItemWrapper>
            </AboutViewAnchored>
        </AboutViewFloating>
    );
}
