import { FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { FormGroup } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { useAppSelector } from "components/store";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SelectOption } from "components/common/select/Select";
import { useRef } from "react";
import ReactAce from "react-ace/lib/ace";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import AceEditor from "components/common/ace/AceEditor";
import Code from "components/common/Code";

export default function EditGenAiTaskContextFields() {
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    const scriptRef = useRef<ReactAce>(null);

    return (
        <>
            <FormGroup>
                <FormLabel>
                    Source collection
                    <PopoverWithHoverWrapper message="Select the collection to use as the source of documents for the task.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete control={control} name="collectionName" options={collectionOptions} />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Context generation script
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Use <code>ai.genContext</code> in this script to generate a{" "}
                                <strong>context object</strong> from the source document.
                                <br />
                                Each context object will be passed as a separate input to the model.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormAceEditor
                    aceRef={scriptRef}
                    control={control}
                    name="script"
                    mode="javascript"
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        {
                            component: (
                                <AceEditor.LoadFileAction
                                    onLoad={(value) => setValue("script", value, { shouldValidate: true })}
                                />
                            ),
                        },
                        {
                            component: <AceEditor.HelpAction message={<ScriptSyntaxHelp />} />,
                            position: "bottom",
                        },
                    ]}
                />
            </FormGroup>
        </>
    );
}

function ScriptSyntaxHelp() {
    const code = `for(const comment of this.Comments)  // 'this' is the source document
{
    // Call 'ai.genContext' to generate a context object for each comment.
    // The custom object passed to this method defines the structure of the context object.
    ai.genContext({
        Text: \`Blog post topic: \${this.Topic}. Comment: \${comment.Text}\`, 
        AuthorName: comment.Author,
        CommentId: comment.Id
    });
}`;

    const codeWithAttachments = `const banana = "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";

for(const comment of this.Comments)
{
    ai.genContext({ Id: comment.Id })
        .withPng(banana)
        .withPng(loadAttachment("heart.png"))
        .withText(loadAttachment("transactions.csv"))
        .withText(loadAttachment("text.txt"));
}`;

    return (
        <div>
            <div>Sample context generation script</div>
            <Code code={code} language="javascript" elementToCopy={code} />
            <div className="mt-2">With attachments</div>
            <Code code={codeWithAttachments} language="javascript" elementToCopy={codeWithAttachments} />
        </div>
    );
}
