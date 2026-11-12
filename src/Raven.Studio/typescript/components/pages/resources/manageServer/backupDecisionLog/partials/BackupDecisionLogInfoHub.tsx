import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function BackupDecisionLogInfoHub() {
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
                    The backup scheduler decides on each node when backups start, wait, get held back, fail, complete or
                    get cancelled. This view shows those decisions, so you can see why a backup did or did not run.
                </p>
                <ul>
                    <li className="margin-top-xxs">Each node keeps its own log of decisions.</li>
                    <li className="margin-top-xxs">
                        Repeated identical decisions are collapsed into a single row with a counter.
                    </li>
                </ul>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
