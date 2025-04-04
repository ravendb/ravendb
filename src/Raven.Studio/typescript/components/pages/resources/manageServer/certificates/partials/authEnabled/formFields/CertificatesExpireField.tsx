import { FormInput, FormSelect, FormGroup, FormLabel } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { CertificatesRegenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRegenerateModal";
import { ExpireTimeUnit } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { useFormContext } from "react-hook-form";

export default function CertificatesExpireField() {
    const { control } = useFormContext<
        CertificatesGenerateFormData | CertificatesCloneFormData | CertificatesRegenerateFormData
    >();

    return (
        <FormGroup>
            <FormLabel>Expire in</FormLabel>
            <FormInput
                type="number"
                control={control}
                name="expireIn"
                placeholder="Validity period (Default: 60 months)"
                addon={
                    <FormSelect
                        control={control}
                        name="expireTimeUnits"
                        options={
                            [
                                { value: "days", label: "Days" },
                                { value: "months", label: "Months" },
                            ] satisfies SelectOption<ExpireTimeUnit>[]
                        }
                    />
                }
            />
        </FormGroup>
    );
}
