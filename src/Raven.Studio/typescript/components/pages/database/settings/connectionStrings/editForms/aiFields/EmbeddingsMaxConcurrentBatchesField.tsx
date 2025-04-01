import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useFormContext } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormInput, FormLabel } from "components/common/Form";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import OptionalLabel from "components/common/OptionalLabel";

type FormData = ConnectionFormData<AiConnection>;

export default function EmbeddingsMaxConcurrentBatches({ baseName }: { baseName: FormData["connectorType"] }) {
    const { control } = useFormContext<FormData>();

    return (
        <div className="mb-2">
            <FormLabel>
                Max Concurrent Query Batches <OptionalLabel />
                <PopoverWithHoverWrapper message="Maximum number of query embedding batches that can be processed concurrently.">
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormInput control={control} name={`${baseName}.embeddingsMaxConcurrentBatches`} type="number" />
        </div>
    );
}
