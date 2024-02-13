import { CreateDatabaseFromBackupFormData } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import { CreateDatabaseRegularFormData } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularValidation";
import moment from "moment";

export function useCreateDatabase(formValues: CreateDatabaseRegularFormData | CreateDatabaseFromBackupFormData) {
    const encryptionKeyFileName = `Key-of-${formValues.basicInfo.databaseName}-${moment().format(
        "YYYY-MM-DD-HH-mm"
    )}.txt`;

    const encryptionKeyText = `Encryption Key for database '${formValues.basicInfo.databaseName}': ${formValues.encryption.key}\r\n\r\nThis key is used to encrypt the RavenDB database, it is required for restoring the database.\r\nMake sure you keep it in a private, safe place as it will Not be available again !`;

    return {
        encryptionKeyFileName,
        encryptionKeyText,
    };
}
