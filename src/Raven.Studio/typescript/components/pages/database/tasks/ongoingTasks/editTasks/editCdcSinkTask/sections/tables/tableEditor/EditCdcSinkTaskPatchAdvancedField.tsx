import { FormAceEditor, FormGroup, FormLabel } from "components/common/Form";
import {
    EmbeddedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import AceEditor from "components/common/ace/AceEditor";
import Code from "components/common/Code";

export default function EditCdcSinkTaskPatchAdvancedField({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <FormGroup marginClass="mb-0">
            <FormLabel>
                Patch
                <PopoverWithHoverWrapper
                    message={
                        <>
                            <p>
                                A patch is a JavaScript snippet that runs on <strong>INSERT</strong> and{" "}
                                <strong>UPDATE</strong> AFTER column mapping is applied and before the document is
                                written to RavenDB.
                            </p>
                            <p>
                                Use it when the mapped document needs additional changes, such as computed fields,
                                aggregate values, metadata updates, or data loaded from related documents.
                            </p>
                        </>
                    }
                >
                    <Icon icon="info-new" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormAceEditor
                control={control}
                name={`${path}.patch`}
                mode="javascript"
                height="150px"
                placeholder={patchPlaceholder}
                actions={[
                    { component: <AceEditor.FullScreenAction /> },
                    { component: <AceEditor.FormatAction /> },
                    {
                        component: <AceEditor.HelpAction message={patchSyntaxHelp} />,
                        position: "bottom",
                    },
                ]}
                isFullScreenLabelHidden
            />
        </FormGroup>
    );
}

const patchPlaceholder = `// Optional. Transforms 'this' (the document) AFTER column mapping for INSERT and UPDATE events.
// e.g. this.FullName = $row.first_name + ' ' + $row.last_name;
// Click the (?) icon on the right for syntax help and more examples.`;

const ComputeFieldSyntaxHelp = () => {
    const code = `// $row contains the raw CDC row with all columns as-is from the source database
this.FullName = ($row.first_name + ' ' + $row.last_name).trim();`;

    return (
        <div>
            <div className="mb-1">
                <strong>Compute a field from source columns:</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const UnmappedColumnSyntaxHelp = () => {
    const code = `// (base_price and tax_rate are not in the column mapping)
this.FinalPrice = $row.base_price * (1 + $row.tax_rate);`;

    return (
        <div>
            <div className="mb-1">
                <strong>Computed field from an unmapped columns:</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const DeltaComputationSyntaxHelp = () => {
    const code = `// Use $old to update a running total or compute a value based on the previous state of the document. 
const oldAmount = $old?.Amount || 0;
const newAmount = $row.amount || 0;
this.RunningTotal = (this.RunningTotal || 0) + (newAmount - oldAmount);`;

    return (
        <div>
            <div className="mb-1">
                <strong>
                    Delta computation using <code>$old</code>:
                </strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const ConditionalEnrichmentSyntaxHelp = () => {
    const code = `if ($row.status === 'VIP') {
    this.Priority = 'High';
    this.Discount = 0.15;
}`;

    return (
        <div>
            <div className="mb-1">
                <strong>Conditionally enrich the document:</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const LoadRelatedDocumentSyntaxHelp = () => {
    const code = `const customer = load('Customers/' + $row.customer_id);
if (customer) {
    this.CustomerName = customer.Name;
    this.CustomerEmail = customer.Email;
}`;

    return (
        <div>
            <div className="mb-1">
                <strong>Load a related document:</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const VariablesLegend = () => (
    <div className="border border-secondary rounded-2 panel-bg-2 p-3">
        <div className="mb-2">
            <strong>Available variables</strong>
        </div>
        <ul className="mb-0">
            <li>
                <code>this</code> - the document AFTER column mapping has been applied
                <br />
                (already contains the new values from the CDC row).
            </li>
            <li>
                <code>$row</code> - the raw CDC row with all columns as-is from the source database.
            </li>
            <li>
                <code>$old</code> - the state stored in RavenDB BEFORE the CDC event was processed
                <br />(<strong>for root patches</strong>: the full document; <strong>for embedded patches</strong>: the
                matched embedded item).
                <br />
                Null for inserts.
            </li>
        </ul>
    </div>
);

const patchSyntaxHelp = (
    <div className="vstack gap-2">
        <div className="mb-2">
            <VariablesLegend />
        </div>
        <ComputeFieldSyntaxHelp />
        <UnmappedColumnSyntaxHelp />
        <DeltaComputationSyntaxHelp />
        <ConditionalEnrichmentSyntaxHelp />
        <LoadRelatedDocumentSyntaxHelp />
    </div>
);
