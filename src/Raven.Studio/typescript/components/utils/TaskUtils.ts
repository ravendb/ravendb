import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;
import OngoingTaskType = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

export default class TaskUtils {
    static etlTypeToTaskType(etlType: EtlType): OngoingTaskType {
        switch (etlType) {
            case "ElasticSearch":
                return "ElasticSearchEtl";
            case "Olap":
                return "OlapEtl";
            case "Raven":
                return "RavenEtl";
            case "Sql":
                return "SqlEtl";
            default:
                throw new Error("Unknown etl type mapping: " + etlType);
        }
    }

    static taskTypeToEtlType(taskType: OngoingTaskType): EtlType {
        switch (taskType) {
            case "RavenEtl":
                return "Raven";
            case "OlapEtl":
                return "Olap";
            case "ElasticSearchEtl":
                return "ElasticSearch";
            case "SqlEtl":
                return "Sql";
            default:
                throw new Error("Unsupported task type: " + taskType);
        }
    }
}
