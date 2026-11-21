import { useFormContext, useWatch } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import useBoolean from "components/hooks/useBoolean";
import { DestinationType, FormDestinationData, FormDestinations } from "./utils/formDestinationsTypes";
import { formDestinationsUtils } from "./utils/formDestinationsUtils";

type NodeConnectionTestResult = Raven.Server.Web.System.NodeConnectionTestResult;

interface DestinationTest {
    type: DestinationType;
    destination: FormDestinationData;
    result: NodeConnectionTestResult;
}

export type DestinationTestState = ReturnType<typeof useDestinationTest>;

export function useDestinationTest(type: DestinationType) {
    const { control, trigger, getValues } = useFormContext<FormDestinations>();
    const currentDestination = useWatch({ control, name: `destinations.${type}` });
    const { manageServerService } = useServices();
    const { value: isErrorModalOpen, setValue: setIsErrorModalOpen, setFalse: closeErrorModal } = useBoolean(false);

    const asyncTest = useAsyncCallback(async (): Promise<DestinationTest> => {
        const isValid = await trigger(`destinations.${type}`);
        if (!isValid) {
            return null;
        }

        const { connectionType, mapToDto } = formDestinationsUtils.DESTINATION_TYPE_INFO[type];
        const destination = _.cloneDeep(getValues(`destinations.${type}`));

        const result = await manageServerService.testPeriodicBackupCredentials(
            connectionType,
            mapToDto({ ...destination, isEnabled: true })
        );
        setIsErrorModalOpen(!!result.Error);
        return { type, destination, result };
    });

    const test = asyncTest.result;
    const isTestOfCurrentDestination = test?.type === type && _.isEqual(test.destination, currentDestination);
    const result = isTestOfCurrentDestination ? test.result : null;

    return {
        execute: asyncTest.execute,
        loading: asyncTest.loading,
        result,
        isErrorModalOpen: isErrorModalOpen && !!result?.Error,
        closeErrorModal,
    };
}
