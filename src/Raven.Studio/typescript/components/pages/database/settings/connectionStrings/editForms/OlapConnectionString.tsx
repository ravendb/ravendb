import Form from "react-bootstrap/Form";
import { FormInput, FormLabel } from "components/common/Form";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { ConnectionFormData, EditConnectionStringFormProps, OlapConnection } from "../connectionStringsTypes";
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
import { useAppUrls } from "components/hooks/useAppUrls";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
import { useAppSelector } from "components/store";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";
import * as yup from "yup";

type FormData = ConnectionFormData<OlapConnection>;

interface OlapConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: OlapConnection;
}

export default function OlapConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: OlapConnectionStringProps) {
    const usedNames = useAppSelector(connectionStringSelectors.connections)["Olap"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const form = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
        context: {
            isForNewConnection,
            usedNames,
        } satisfies ConnectionStringsNameContext,
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
            <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)}>
                <div className="mb-4">
                    <FormLabel>Name</FormLabel>
                    <FormInput
                        control={control}
                        name="name"
                        type="text"
                        placeholder="Enter a name for the connection string"
                        disabled={!isForNewConnection}
                        autoComplete="off"
                    />
                </div>
                <FormDestinationList isForNewConnection={isForNewConnection} />
                {isServerWide && <ExcludedDatabasesFormSelect control={control} name="excludedDatabases" />}
            </Form>

            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editOlapEtl}
            />
        </FormProvider>
    );
}

const schema = yupObjectSchema<Omit<FormData, "destinations">>({
    name: connectionStringsUtils.nameSchema,
    excludedDatabases: yup.array().of(yup.string()).optional(),
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
