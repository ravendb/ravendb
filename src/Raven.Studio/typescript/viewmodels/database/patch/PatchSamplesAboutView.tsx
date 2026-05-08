import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";

export default function PatchSamplesAboutView() {
    const docsLink = useRavenLink({ hash: "TODO" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>TODO</p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={docsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Patch
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
