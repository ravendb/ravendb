import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";

type CdcSinkConfiguration = Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration;

function mapFromDto(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink): EditCdcSinkTaskFormData {
    if (!dto) {
        return {
            name: "",
            state: "Enabled",
            isSetResponsibleNode: false,
            responsibleNode: "",
            isPinResponsibleNode: false,
            connectionStringName: "",
            skipInitialLoad: false,
            postgresPublicationName: "",
            postgresSlotName: "",
            tables: [],
        };
    }

    const configuration = dto.Configuration;

    return {
        name: configuration.Name,
        state: configuration.Disabled ? "Disabled" : "Enabled",
        isSetResponsibleNode: Boolean(configuration.MentorNode),
        responsibleNode: configuration.MentorNode ?? "",
        isPinResponsibleNode: configuration.PinToMentorNode ?? false,
        connectionStringName: configuration.ConnectionStringName ?? "",
        skipInitialLoad: configuration.SkipInitialLoad ?? false,
        postgresPublicationName: configuration.Postgres?.PublicationName ?? "",
        postgresSlotName: configuration.Postgres?.SlotName ?? "",
        tables: configuration.Tables ?? [],
    };
}

function mapToDto(formData: EditCdcSinkTaskFormData, taskId: number): CdcSinkConfiguration {
    const hasPostgresSettings = formData.postgresPublicationName || formData.postgresSlotName;
    const postgresSettings: CdcSinkConfiguration["Postgres"] = hasPostgresSettings
        ? {
              PublicationName: formData.postgresPublicationName,
              SlotName: formData.postgresSlotName,
          }
        : null;

    return {
        TaskId: taskId,
        Name: formData.name,
        Disabled: formData.state === "Disabled",
        ConnectionStringName: formData.connectionStringName,
        MentorNode: formData.isSetResponsibleNode ? formData.responsibleNode : null,
        PinToMentorNode: formData.isPinResponsibleNode,
        Postgres: postgresSettings,
        SkipInitialLoad: formData.skipInitialLoad,
        Tables: formData.tables as Raven.Client.Documents.Operations.CdcSink.CdcSinkTableConfig[],
    };
}

export const editCdcSinkTaskUtils = {
    mapFromDto,
    mapToDto,
};
