import { useFormContext } from "react-hook-form";
import { FormSelect, FormSelectCreatable } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface AwsRegionFieldProps {
    name: "destinations.s3.awsRegionName" | "destinations.glacier.awsRegionName";
    availableRegions: SelectOption[];
    allowedRegions?: string[];
}

export default function AwsRegionField({ name, availableRegions, allowedRegions }: AwsRegionFieldProps) {
    const { control } = useFormContext<FormDestinations>();
    const t = useStudioTranslation("formDestinations");

    if (allowedRegions?.length > 0) {
        const allowedOptions = allowedRegions.map(
            (region) => availableRegions.find((x) => x.value === region) ?? { value: region, label: region }
        );

        return (
            <FormSelect
                name={name}
                control={control}
                placeholder={t("aws.regionSelectPlaceholder")}
                options={allowedOptions}
            />
        );
    }

    return (
        <FormSelectCreatable
            name={name}
            control={control}
            placeholder={t("aws.regionCreatablePlaceholder")}
            options={availableRegions}
        />
    );
}
