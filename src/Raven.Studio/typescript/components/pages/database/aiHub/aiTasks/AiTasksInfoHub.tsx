import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useRavenLink } from "components/hooks/useRavenLink";
import { Icon } from "components/common/Icon";

export default function AiTasksInfoHub() {
    const ongoingTasksDocsLink = useRavenLink({ hash: "K4ZTNA" });

    // TODO adjust to only AI tasks
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
                    <strong>Ongoing-tasks</strong> are work tasks assigned to the database.
                    <ul className="margin-top-xxs">
                        <li>
                            A few examples are: <br />
                            Executing a periodic backup of the database, replicating to another RavenDB instance, or
                            transferring data to external frameworks such as Kafka, RabbitMQ, Azure Queue Storage etc.
                        </li>
                        <li className="margin-top-xxs">
                            Click the &quot;Add a Database Task&quot; button to view all available tasks and select from
                            the list.
                        </li>
                    </ul>
                </div>
                <div>
                    <strong>Running in the background</strong>, each ongoing task is handled by a designated node from
                    the Database-Group nodes.
                    <ul className="margin-top-xxs">
                        <li>
                            For each task, you can specify which node will be responsible for the task and whether the
                            cluster may assign a different node when that node is down.
                        </li>
                        <li className="margin-top-xxs">
                            If not specified, the cluster will decide which node will handle the task.
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={ongoingTasksDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Ongoing Tasks
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
