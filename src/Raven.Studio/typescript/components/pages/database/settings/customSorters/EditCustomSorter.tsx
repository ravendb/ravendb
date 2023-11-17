import React, { useEffect, useState } from "react";
import { Button, InputGroup, Label } from "reactstrap";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useRavenLink } from "components/hooks/useRavenLink";
import { FormAceEditor, FormInput } from "components/common/Form";
import { useForm, useWatch } from "react-hook-form";
import LoadFromFile from "components/common/LoadFromFile";
import {
    EditCustomSorterScriptFormData,
    editCustomSorterScriptYupResolver,
} from "components/pages/database/settings/customSorters/EditCustomSorterValidation";
import { useServices } from "hooks/useServices";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;
import { useEventsCollector } from "hooks/useEventsCollector";
import { useAccessManager } from "hooks/useAccessManager";

todo("Feature", "Damian", "Add missing logic");
todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");
todo("Feature", "Damian", "Connect CustomSortersInfoHub (it throws some error in Storybook)");
todo("Other", "Danielle", "Add Info Hub text");

export default function EditCustomSorter() {
    const { databasesService } = useServices();

    // const asyncGetEditCustomSorterScriptConfiguration = useAsyncCallback<EditCustomSorterScriptFormData>(async () =>
    //     mapToFormData(await databasesService.saveCustomSorterCommand())
    // );

    const { handleSubmit, control, formState, reset, setValue } = useForm<EditCustomSorterScriptFormData>({
        resolver: editCustomSorterScriptYupResolver,
        mode: "all",
    });

    useDirtyFlag(formState.isDirty);

    const formValues = useWatch({ control: control });
    const { reportEvent } = useEventsCollector();
    const { isAdminAccessOrAbove } = useAccessManager();

    useEffect(() => {
        if (!formValues.Name && formValues.Name !== null) {
            setValue("Name", null, { shouldValidate: true });
        }
        if (!formValues.Code && formValues.Code !== null) {
            setValue("Code", null, { shouldValidate: true });
        }
    }, [formValues.Name, formValues.Code, setValue]);

    // const onSave: SubmitHandler<EditCustomSorterScriptFormData> = async (formData) => {
    //     return tryHandleSubmit(async () => {
    //         reportEvent("edit-custom-sorter-script", "save");
    //
    //         await databasesService.saveCustomSorter(db, {
    //             Name: formData.Name,,
    //         });
    //
    //         messagePublisher.reportSuccess("Custom sorter script saved successfully");
    //         reset(formData);
    //     });
    // };

    const customSortersDocsLink = useRavenLink({ hash: "LGUJH8" });

    const [isNewScript] = useState(false);

    return (
        <>
            <InputGroup className="vstack mb-1">
                <Label>Name</Label>
                <FormInput
                    control={control}
                    name="Name"
                    placeholder="Enter a sorter name"
                    type="text"
                    disabled={formState.isSubmitting}
                />
            </InputGroup>
            <InputGroup className="vstack">
                <Label className="d-flex flex-wrap justify-content-between">
                    Script
                    <LoadFromFile
                        trigger={
                            <Button color="link" size="xs" className="p-0 align-self-end">
                                <Icon icon="upload" margin="me-1" />
                                Load from a file
                            </Button>
                        }
                        acceptedExtensions={[".cs"]}
                    />
                </Label>
                <FormAceEditor
                    name="Code"
                    control={control}
                    mode={"csharp"}
                    height="400px"
                    // disabled={formState.isSubmitting}
                />
            </InputGroup>
        </>
    );
}

function mapToFormData(dto: SorterDefinition): EditCustomSorterScriptFormData {
    if (!dto) {
        return {
            Name: null,
            Code: null,
        };
    }

    return {
        Name: dto.Name,
        Code: dto.Code,
    };
}
