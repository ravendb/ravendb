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

export default function EmbeddingsMaxConcurrentBatches({
    baseName,
}: {
    baseName: Exclude<FormData["connectorType"], "anthropicSettings">;
}) {
    const { control } = useFormContext<FormData>();

    return (
        <div className="mb-2">
            <FormLabel>
                Max Concurrent Embedding Batches <OptionalLabel />
                <PopoverWithHoverWrapper
                    message={
                        <>
                            Maximum number of concurrent embedding batches for each embeddings generation task that uses
                            this connection string. RavenDB can run up to this number of source-document batches and up
                            to the same number of search-term batches at the same time.
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormInput control={control} name={`${baseName}.embeddingsMaxConcurrentBatches`} type="number" />
        </div>
    );
}
