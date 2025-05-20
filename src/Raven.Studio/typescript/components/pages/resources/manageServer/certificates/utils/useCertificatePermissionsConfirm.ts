import useConfirm from "components/common/ConfirmDialog";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";

export default function useCertificatePermissionsConfirm() {
    const confirm = useConfirm();

    return async (formData: CertificatesGenerateFormData) => {
        if (formData.securityClearance === "ValidUser" && formData.databasePermissions.length === 0) {
            return await confirm({
                title: "Did you forget to assign database permissions?",
                message:
                    "Leaving the database privileges section empty is going to prevent users from accessing the database.",
                confirmText: "Save anyway",
            });
        }

        return true;
    };
}
