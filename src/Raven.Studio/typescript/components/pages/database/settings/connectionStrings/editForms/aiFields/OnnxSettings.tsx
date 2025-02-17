import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { FormSelect, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import { Label, UncontrolledPopover, PopoverBody } from "reactstrap";

type FormData = ConnectionFormData<AiConnection>;

export default function OnnxSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("onnxSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "Onnx", {
            CaseSensitive: formValues.onnxSettings.caseSensitive,
            NormalizeEmbeddings: formValues.onnxSettings.normalizeEmbeddings,
            MaximumTokens: formValues.onnxSettings.maximumTokens,
            ClsToken: formValues.onnxSettings.clsToken,
            PadToken: formValues.onnxSettings.padToken,
            SepToken: formValues.onnxSettings.sepToken,
            UnknownToken: formValues.onnxSettings.unknownToken,
            PoolingMode: formValues.onnxSettings.poolingMode,
            UnicodeNormalization: formValues.onnxSettings.unicodeNormalization,
        });
    });

    return (
        <>
            <div className="mb-2">
                <Label>
                    Case Sensitive <OptionalLabel />
                    <Icon icon="info" color="info" id="caseSensitive" margin="ms-1" />
                    <UncontrolledPopover target="caseSensitive" trigger="hover" className="bs5">
                        <PopoverBody>The flag to indicate whether the model should be case-sensitive.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormSelect
                    control={control}
                    name="onnxSettings.caseSensitive"
                    disabled={isUsedByAnyTask}
                    options={booleanSelectOptions}
                    isClearable
                />
            </div>
            <div className="mb-2">
                <Label>
                    Normalize Embeddings <OptionalLabel />
                    <Icon icon="info" color="info" id="normalizeEmbeddings" margin="ms-1" />
                    <UncontrolledPopover target="normalizeEmbeddings" trigger="hover" className="bs5">
                        <PopoverBody>
                            Sets whether the resulting embedding vectors should be explicitly normalized.
                            <br />
                            <br />
                            Normalized embeddings may be compared more efficiently, such as by using a dot product
                            rather than cosine similarity.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormSelect
                    control={control}
                    name="onnxSettings.normalizeEmbeddings"
                    disabled={isUsedByAnyTask}
                    options={booleanSelectOptions}
                    isClearable
                />
            </div>
            <div className="mb-2">
                <Label>
                    Maximum Tokens <OptionalLabel />
                    <Icon icon="info" color="info" id="maximumTokens" margin="ms-1" />
                    <UncontrolledPopover target="maximumTokens" trigger="hover" className="bs5">
                        <PopoverBody>The maximum number of tokens that the model can process.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput
                    control={control}
                    name="onnxSettings.maximumTokens"
                    type="number"
                    disabled={isUsedByAnyTask}
                />
            </div>
            <div className="mb-2">
                <Label>
                    CLS Token <OptionalLabel />
                    <Icon icon="info" color="info" id="clsToken" margin="ms-1" />
                    <UncontrolledPopover target="clsToken" trigger="hover" className="bs5">
                        <PopoverBody>
                            Defaults to &quot;[CLS]&quot;.
                            <br />
                            <br />
                            The CLS token is a special token that is added to the beginning of the input sequence. It is
                            used to represent the classification of the entire input sequence.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="onnxSettings.clsToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    Pad Token <OptionalLabel />
                    <Icon icon="info" color="info" id="padToken" margin="ms-1" />
                    <UncontrolledPopover target="padToken" trigger="hover" className="bs5">
                        <PopoverBody>
                            The PAD token is a special token that is used to pad the input sequence to a fixed length.
                            It is used to handle input sequences that are shorter than the maximum sequence length.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="onnxSettings.padToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    SEP Token <OptionalLabel />
                    <Icon icon="info" color="info" id="sepToken" margin="ms-1" />
                    <UncontrolledPopover target="sepToken" trigger="hover" className="bs5">
                        <PopoverBody>
                            The SEP token is a special token that is added to the end of the input sequence. It is used
                            to separate the input sequence from the classification label.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="onnxSettings.sepToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    Unknown Token <OptionalLabel />
                    <Icon icon="info" color="info" id="unknownToken" margin="ms-1" />
                    <UncontrolledPopover target="unknownToken" trigger="hover" className="bs5">
                        <PopoverBody>
                            The UNK token is a special token that is used to represent unknown words in the input
                            sequence. It is used to handle out-of-vocabulary words.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="onnxSettings.unknownToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    Pooling Mode <OptionalLabel />
                    <Icon icon="info" color="info" id="poolingMode" margin="ms-1" />
                    <UncontrolledPopover target="poolingMode" trigger="hover" className="bs5">
                        <PopoverBody>
                            Pooling mode to use when generating the fixed-length embedding result.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormSelect
                    control={control}
                    name="onnxSettings.poolingMode"
                    options={
                        [
                            { label: "Max", value: "Max" },
                            { label: "Mean", value: "Mean" },
                            { label: "MeanSquareRootTokensLength", value: "MeanSquareRootTokensLength" },
                        ] satisfies SelectOption<FormData["onnxSettings"]["poolingMode"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                    isClearable
                />
            </div>
            <div className="mb-2">
                <Label>
                    Unicode Normalization <OptionalLabel />
                    <Icon icon="info" color="info" id="unicodeNormalization" margin="ms-1" />
                    <UncontrolledPopover target="unicodeNormalization" trigger="hover" className="bs5">
                        <PopoverBody>
                            Type of Unicode normalization to perform on input text.
                            <br />
                            <br />
                            Unicode normalization is the process of transforming input text into a standard form that
                            can be more easily compared. The normalization form determines the specific normalization
                            rules that are applied to the input text.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormSelect
                    control={control}
                    name="onnxSettings.unicodeNormalization"
                    options={
                        [
                            { label: "FormC", value: "FormC" },
                            { label: "FormD", value: "FormD" },
                            { label: "FormKC", value: "FormKC" },
                            { label: "FormKD", value: "FormKD" },
                        ] satisfies SelectOption<FormData["onnxSettings"]["unicodeNormalization"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                    isClearable
                />
            </div>
            <div className="d-flex mb-2">
                <FlexGrow />
                <ButtonWithSpinner
                    color="secondary"
                    icon="rocket"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                >
                    Test connection
                </ButtonWithSpinner>
            </div>
            {asyncTest.result && <ConnectionTestResult testResult={asyncTest.result} />}
        </>
    );
}

const booleanSelectOptions: SelectOption<boolean>[] = [
    { value: true, label: "Yes" },
    { value: false, label: "No" },
];
