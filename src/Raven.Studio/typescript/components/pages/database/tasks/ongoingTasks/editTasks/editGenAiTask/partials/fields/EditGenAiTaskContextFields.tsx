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

    return (
        <div>
            <div>Sample context generation script</div>
            <Code code={code} language="javascript" elementToCopy={code} />
        </div>
    );
}
