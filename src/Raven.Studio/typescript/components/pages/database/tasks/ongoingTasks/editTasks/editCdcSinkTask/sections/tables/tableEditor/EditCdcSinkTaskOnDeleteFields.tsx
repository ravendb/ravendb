import { FormAceEditor, FormGroup, FormLabel, FormSwitch } from "components/common/Form";
import {
    EmbeddedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext } from "react-hook-form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Code from "components/common/Code";
import AceEditor from "components/common/ace/AceEditor";

const VariablesLegend = () => (
    <div className="border border-secondary rounded-2 panel-bg-2 p-3">
        <div className="mb-2">
            <strong>Available variables</strong>
        </div>
        <ul className="mb-0">
            <li>
                <code>this</code> -<br />
                <strong>root deletes</strong>: the document about to be deleted.
                <br />
                <strong>Embedded deletes</strong>: the parent document, with the embedded item already removed.
                <br />
                If <em>Ignore deletes</em> is on, the item is still there - the deletion hasn&apos;t been applied yet.
            </li>
            <li>
                <code>$row</code> -<br />
                the raw CDC row from the DELETE event, with all columns as-is from the source database.
            </li>
            <li>
                <code>$old</code> -<br />
                <strong>root deletes</strong>: the full document (same as <code>this</code>).
                <br />
                <strong>Embedded deletes</strong>: the embedded item being removed.
                <br />
                Always populated on DELETE.
            </li>
        </ul>
    </div>
);

const ArchiveSyntaxHelp = () => {
    const code = `// Combine with "Ignore deletes" to keep the document instead of removing it.
this.Archived = true;
this.ArchivedAt = new Date().toISOString();`;

    return (
        <div>
            <div className="mb-1">
                <strong>Archive the document instead of deleting it:</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const AuditTrailSyntaxHelp = () => {
    const code = `// Use put() to persist an audit record before the document is deleted.
put('DeletedOrders/' + id(this), {
    OriginalId: id(this),
    Customer: this.Customer,
    Total: this.Total,
    DeletedAt: new Date().toISOString()
});`;

    return (
        <div>
            <div className="mb-1">
                <strong>Write an audit record before deletion:</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const RecomputeAggregateSyntaxHelp = () => {
    const code = `// For embedded deletes (without "Ignore deletes"): 
// the item is already removed from this.Lines,
// so re-summing gives the correct post-deletion total.
this.TotalQuantity = (this.Lines || [])
    .reduce((sum, line) => sum + (line.Quantity || 0), 0);`;

    return (
        <div>
            <div className="mb-1">
                <strong>Recompute a parent aggregate after an embedded item is removed:</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const deletePatchSyntaxHelp = (
    <div className="vstack gap-2">
        <div className="mb-2">
            <VariablesLegend />
        </div>
        <ArchiveSyntaxHelp />
        <AuditTrailSyntaxHelp />
        <RecomputeAggregateSyntaxHelp />
    </div>
);

const deletePatchPlaceholder = `// Optional. Runs on DELETE events from the source BEFORE the delete is applied or ignored.
// e.g.  this.Archived = true;   // combine with "Ignore deletes" for soft-delete
// Click the (?) icon below for syntax and more examples.`;

export default function EditCdcSinkTaskOnDeleteFields({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <div>
            <FormGroup>
                <FormSwitch control={control} name={`${path}.onDelete.ignoreDeletes`}>
                    Ignore deletes
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    <strong>Default (OFF):</strong>
                                    <br />
                                    DELETE events from the source are applied to RavenDB - root-table documents are
                                    deleted, embedded items are removed from the parent document, and <code>Value</code>
                                    -type embedded properties are set to <code>null</code>.
                                </p>
                                <p>
                                    <strong>When enabled:</strong>
                                    <br />
                                    DELETE events are <strong>not applied</strong>. The document, embedded item, or{" "}
                                    <code>Value</code>-type property is kept.
                                </p>
                                <p>
                                    If a delete patch is configured, it still runs BEFORE the delete decision, even when{" "}
                                    <em>Ignore deletes</em> is on.
                                    <br />
                                    You can use it to mark the document as archived,
                                    <br />
                                    for example: <code>this.Archived = true</code>.
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormSwitch>
            </FormGroup>
            <FormGroup marginClass="mb-0">
                <FormLabel>
                    Delete patch
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    A delete patch is a JavaScript snippet that runs on <strong>DELETE</strong> events
                                    from the source, BEFORE the delete is applied or ignored.
                                </p>
                                <p>
                                    Use it when a delete should trigger additional changes, such as marking documents as
                                    archived (combined with <em>Ignore deletes</em>), writing an audit record with{" "}
                                    <code>put()</code>, or reversing parent-level aggregates when an embedded item is
                                    removed.
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormAceEditor
                    control={control}
                    name={`${path}.onDelete.patch`}
                    mode="javascript"
                    height="150px"
                    placeholder={deletePatchPlaceholder}
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        {
                            component: <AceEditor.HelpAction message={deletePatchSyntaxHelp} />,
                            position: "bottom",
                        },
                    ]}
                    isFullScreenLabelHidden
                />
            </FormGroup>
        </div>
    );
}
