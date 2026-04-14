import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";

export default function ServerWideConnectionStringsInfoHub() {
    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    <ul>
                        <li>
                            RavenDB is designed to interact with diverse data storage solutions via replication, ETL, or
                            incoming data processing.
                        </li>
                        <li className="margin-top-xxs">
                            From this view, you can manage connection strings that will be available across{" "}
                            <strong>ALL databases</strong> in your cluster.
                        </li>
                        <li className="margin-top-xxs">
                            Server-wide connection strings can be referenced when defining ongoing tasks in any database.
                        </li>
                        <li className="margin-top-xxs">
                            Connection strings that are in use by ongoing tasks cannot be deleted, as they are essential
                            for task functionality and data access.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
