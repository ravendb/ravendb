import { FormSelect, FormGroup, FormLabel } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { useAppSelector } from "components/store";
import { useFormContext } from "react-hook-form";

export default function CertificatesSecurityClearanceField() {
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const { control } = useFormContext<
        CertificatesGenerateFormData | CertificatesCloneFormData | CertificatesEditFormData | CertificatesEditFormData
    >();

    return (
        <FormGroup>
            <FormLabel>Security Clearance</FormLabel>
            <FormSelect
                control={control}
                name="securityClearance"
                options={
                    [
                        {
                            value: "ClusterAdmin",
                            label: "Cluster Admin",
                            isDisabled: !isClusterAdminOrClusterNode,
                        },
                        { value: "Operator", label: "Operator" },
                        { value: "ValidUser", label: "User" },
                    ] satisfies SelectOption[]
                }
            />
        </FormGroup>
    );
}
