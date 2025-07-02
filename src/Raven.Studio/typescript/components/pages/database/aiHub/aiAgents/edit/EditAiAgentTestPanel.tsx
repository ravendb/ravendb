import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "./utils/editAiAgentValidation";
import { useAppSelector } from "components/store";
import { editAiAgentSelectors } from "./store/editAiAgentSlice";
import { FormInput } from "components/common/Form";
import Button from "react-bootstrap/Button";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export default function EditAiAgentTestPanel() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    console.log("kalczur ", formValues);

    const isTestOpen = useAppSelector(editAiAgentSelectors.isTestOpen);

    const { aiAgentService } = useServices();

    const asyncHandleTest = useAsyncCallback(async () => {
        // const dto: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration & {
        //     Parameters: TODO;
        //     Prompt: string;
        // } = {
        //     ConnectionStringName: formValues.connectionStringName,
        //     SystemPrompt: formValues.systemPrompt,
        //     OutputSchema: formValues.outputSchema,
        //     Parameters: { company: "companies/90-A" }, // TODO
        //     Prompt: formValues.prompt,
        //     Persistence: {
        //         Collection: formValues.persistenceExpires,
        //         Expires: "3.00:00:00", // TODO
        //     },
        //     Queries: formValues.queries.map((x) => ({
        //         Name: x.name,
        //         Description: x.description,
        //         Query: x.query,
        //         ParametersSchema: x.parametersSchema,
        //     })),
        // };
        // const result = await aiAgentService.testAiAgent(databaseName, dto);
        // return result;
    });

    return (
        <>
            <div className="panel-bg-2 p-3 border-bottom border-secondary">
                <h3 className="m-0">
                    <Icon icon="test" color="primary" />
                    Test results
                </h3>
            </div>
            {!isTestOpen && (
                <div className="p-3 flex-grow-1 vstack justify-content-center align-items-center">
                    <Icon icon="test" color="primary" className="fs-1" />
                    <p className="mt-2 text-center">
                        This is a testing environment for your AI Agent. Once everything is configured, click the “Test”
                        button to see the results.
                    </p>
                </div>
            )}
            {isTestOpen && (
                <div className="flex-grow-1 vstack justify-content-center align-items-center">
                    <div className="flex-grow-1">Response</div>
                    <div className="hstack w-100 p-2 panel-bg-2 border-top border-secondary">
                        <FormInput type="text" control={control} name="prompt" placeholder="Message an agent" />
                        <Button variant="primary" onClick={asyncHandleTest.execute}>
                            <Icon icon="arrow-up" margin="m-0" />
                        </Button>
                    </div>
                </div>
            )}
        </>
    );
}
