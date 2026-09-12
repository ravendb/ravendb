import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";

export default function PatchSamplesAboutView() {
    const patchViewDocsLink = useRavenLink({ hash: "VXSYQ7" });
    const setBasedPatchOperationsDocsLink = useRavenLink({ hash: "E928D4" });

    return (
        <AboutViewFloating className="me-2">
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    <strong>Patching</strong> lets you modify existing documents by running a script directly on the
                    server, without pulling the documents to the client. <strong>From this view</strong>, you can apply
                    such updates to multiple documents in a single operation.
                </p>
                <div>
                    <strong>A patch script consists of two parts</strong>:
                    <ul className="mt-1">
                        <li>
                            <strong>The QUERY</strong>:
                            <br />
                            An RQL query that defines the set of documents to update, using the same syntax you would
                            use when querying the database or indexes for data retrieval.
                        </li>
                        <li>
                            <strong>The UPDATE</strong>:
                            <br />A JavaScript block inside the <code>update</code> clause that defines the
                            modifications to apply to the documents returned by the query.
                        </li>
                    </ul>
                </div>
                <div>
                    <strong>Basic structure</strong>:
                    <Code code={patchStructureExample} language="rql" className="mt-1 ms-2" isRunQueryHidden />
                </div>
                <div className="mt-3">
                    <strong>Testing the patch script</strong>:
                    <ul className="mt-1">
                        <li>
                            Before applying any changes, you can click <strong>Test</strong> to preview the update
                            section&apos;s effect on a selected document. The original document remains unchanged.
                        </li>
                        <li>
                            During testing, the query is ignored; only the <strong>update</strong> section is executed.
                        </li>
                    </ul>
                </div>
                <div>
                    <strong>Running the patch script</strong>:
                    <ul className="mt-1">
                        <li>
                            When the patch runs, the server executes the query and applies the update clause to each
                            document returned by the query.
                        </li>
                        <li>
                            While the patch is running, a progress dialog lets you track the operation or abort it.
                            Documents already modified before aborting are not reverted.
                            <br />
                            The operation stops processing further documents.
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={patchViewDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Patch View
                </a>
                <br />
                <a href={setBasedPatchOperationsDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Set-based Patch Operations
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}

const patchStructureExample = `from CollectionOrIndex
where ...
update {
    // JavaScript changes
}`;
