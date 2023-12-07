import { Form, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { ConnectionFormData, EditConnectionStringFormProps, OlapConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { yupObjectSchema } from "components/utils/yupUtils";
import FormDestinationList from "components/common/formDestinations/FormDestinationList";
import { destinationsSchema } from "components/common/formDestinations/utils/formDestinationsValidation";
import {
    defaultAzureFormData,
    defaultFtpFormData,
    defaultGlacierFormData,
    defaultGoogleCloudFormData,
    defaultLocalFormData,
    defaultS3FormData,
} from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
import { DevTool } from "@hookform/devtools";
import { useAppUrls } from "components/hooks/useAppUrls";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";

type FormData = ConnectionFormData<OlapConnection>;

interface OlapConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: OlapConnection;
}

export default function OlapConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: OlapConnectionStringProps) {
    const form = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const { control, handleSubmit } = form;
    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();

    const handleSave = async () => {
        onSave({
            ...formValues,
            type: "Olap",
        } as OlapConnection);
    };

    return (
        <FormProvider {...form}>
            <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)} className="vstack gap-2">
                <DevTool control={control} />
                <div>
                    <Label className="mb-0 md-label">Name</Label>
                    <FormInput
                        control={control}
                        name="name"
                        type="text"
                        placeholder="Enter a name for the connection string"
                        disabled={!isForNewConnection}
                    />
                </div>
                <FormDestinationList />
            </Form>

            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editOlapEtl}
            />
        </FormProvider>
    );
}

const schema = yupObjectSchema<Omit<FormData, "destinations">>({
    name: yup.string().nullable().required(),
}).concat(destinationsSchema);

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: OlapConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            destinations: {
                local: defaultLocalFormData,
                s3: defaultS3FormData,
                azure: defaultAzureFormData,
                googleCloud: defaultGoogleCloudFormData,
                glacier: defaultGlacierFormData,
                ftp: defaultFtpFormData,
            },
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
