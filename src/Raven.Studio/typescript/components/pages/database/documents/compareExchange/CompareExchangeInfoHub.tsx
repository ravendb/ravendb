import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";

export default function CompareExchangeInfoHub() {
    const docsLink = useRavenLink({ hash: "2BGJN2" });

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
                    <strong>Compare exchange</strong> items are cluster-wide key/value pairs that provide atomic,
                    interlocked compare-exchange operations across the database group.
                </p>
                <p className="mb-0">
                    Their values are kept consistent database-wide via the Raft consensus algorithm.
                </p>
                <hr />
                <p>This view displays all compare exchange items.</p>
                <ul>
                    <li>
                        <strong>&quot;Compare Exchange Key&quot;</strong> column:
                        <br />
                        The unique key of the item. Click the key to edit the item.
                    </li>
                    <li>
                        <strong>&quot;Value&quot;</strong> column:
                        <br />
                        The value stored for this key.
                    </li>
                    <li>
                        <strong>&quot;Metadata&quot;</strong> column:
                        <br />
                        Optional metadata associated with the item.
                    </li>
                    <li>
                        <strong>&quot;Raft Index&quot;</strong> column:
                        <br />
                        The version of the item, as set by the cluster consensus. This is used for concurrency
                        control.
                    </li>
                </ul>
                <p className="mb-0">
                    You can add a new item, filter items by key prefix, and sort the loaded rows.
                    <br />
                    Select and delete items &ndash; selecting all will delete all items matching the current filter.
                </p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={docsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Compare Exchange
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
