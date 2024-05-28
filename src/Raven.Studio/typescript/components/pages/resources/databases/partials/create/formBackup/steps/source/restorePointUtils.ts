import generalUtils from "common/generalUtils";
import { SelectOption } from "components/common/select/Select";
import { RestorePoint } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import moment from "moment";

function mapFromDto(dto: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoint): RestorePoint {
    let backupType = "";
    if (dto.IsSnapshotRestore) {
        if (dto.IsIncremental) {
            backupType = "Incremental ";
        }
        backupType += "Snapshot";
    } else if (dto.IsIncremental) {
        backupType = "Incremental";
    } else {
        backupType = "Full";
    }

    return {
        dateTime: moment(dto.DateTime).format(generalUtils.dateFormat),
        location: dto.Location,
        fileName: dto.FileName,
        isSnapshotRestore: dto.IsSnapshotRestore,
        isIncremental: dto.IsIncremental,
        isEncrypted: dto.IsEncrypted,
        filesToRestore: dto.FilesToRestore,
        databaseName: dto.DatabaseName,
        nodeTag: dto.NodeTag || "-",
        backupType,
    };
}

const unknownDatabaseName = "Unknown Database";

interface RestorePointGroupedOption {
    label: string;
    options: SelectOption<RestorePoint>[];
}

function mapToSelectOptions(
    dto: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints
): RestorePointGroupedOption[] {
    const groups: RestorePointGroupedOption[] = [];

    dto.List.forEach((dtoRestorePoint) => {
        const databaseName = dtoRestorePoint.DatabaseName ?? unknownDatabaseName;

        if (!groups.find((x) => x.label === databaseName)) {
            groups.push({ label: databaseName, options: [] });
        }

        const group = groups.find((x) => x.label === databaseName);

        const restorePointValue = mapFromDto(dtoRestorePoint);
        group.options.push({
            value: restorePointValue,
            label: `${restorePointValue.dateTime}, ${restorePointValue.backupType} Backup`,
        });
    });

    return groups;
}

export const restorePointUtils = {
    mapToSelectOptions,
};
