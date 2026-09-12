import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormGroup, FormLabel, FormSelect } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { getTypeLabel } from "components/pages/database/settings/connectionStrings/ConnectionStringsPanels";
import { StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import {
    connectionStringsActions,
    connectionStringSelectors,
} from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { sortBy } from "lodash";
import { useEffect } from "react";
import InputGroup from "react-bootstrap/InputGroup";
import { FieldValues, FieldPath, UseControllerProps, useController } from "react-hook-form";

type ConnectionTypeProps =
    | {
          type: Exclude<StudioConnectionType, "Ai">;
      }
    | {
          type: Extract<StudioConnectionType, "Ai">;
          modelType: Raven.Client.Documents.Operations.AI.AiModelType;
      };

type FormTaskConnectionStringProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = {
    control: UseControllerProps<TFieldValues>["control"];
    name: TName;
} & ConnectionTypeProps;

export function FormTaskConnectionString<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormTaskConnectionStringProps<TFieldValues, TName>
) {
    const { control, name, type } = props;
    const modelType = "modelType" in props ? props.modelType : undefined;

    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);
    const { value: isNewOpen, toggle: toggleIsNewOpen } = useBoolean(false);

    const connectionStrings = useAppSelector(connectionStringSelectors.connectionsByType(type)).filter((x) => {
        if (type === "Ai" && "modelType" in x) {
            return x.modelType === modelType;
        }
        return true;
    });

    const { field } = useController({
        name,
        control,
    });

    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("task"));
        dispatch(connectionStringsActions.fetchData(databaseName));

        return () => {
            dispatch(connectionStringsActions.reset());
        };
        // Changing the database causes re-mount
    }, []);

    const connectionStringOptions: SelectOption[] = sortBy(Object.values(connectionStrings), (x) =>
        x.name.toUpperCase()
    ).map((x) => ({
        value: x.name,
        label: x.name,
    }));

    const handleConnectionStringSave = async (connectionName: string) => {
        field.onChange(connectionName);
        toggleIsNewOpen();
    };

    return (
        <FormGroup>
            <FormLabel>Connection String</FormLabel>
            <InputGroup>
                <FormSelect
                    control={control}
                    name={name}
                    options={connectionStringOptions}
                    isLoading={loadStatus === "loading"}
                />
                <InputGroup.Text>
                    <ButtonWithSpinner
                        variant="link"
                        className="text-reset px-0"
                        icon="plus"
                        isSpinning={loadStatus === "loading"}
                        onClick={toggleIsNewOpen}
                    >
                        Create a new {getTypeLabel(type)} connection string
                    </ButtonWithSpinner>
                </InputGroup.Text>
            </InputGroup>
            {isNewOpen && (
                <EditConnectionStrings
                    initialConnection={{ type, modelType }}
                    afterSave={handleConnectionStringSave}
                    afterClose={toggleIsNewOpen}
                />
            )}
        </FormGroup>
    );
}
