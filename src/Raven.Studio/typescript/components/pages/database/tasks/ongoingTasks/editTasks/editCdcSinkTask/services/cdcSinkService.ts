import saveCdcSinkCommand from "commands/database/tasks/saveCdcSinkCommand";
import verifyCdcSinkSourceCommand from "commands/database/tasks/verifyCdcSinkSourceCommand";
import testCdcSinkCommand from "commands/database/tasks/testCdcSinkCommand";
import fetchSqlDatabaseSchemaCommand from "commands/database/tasks/fetchSqlDatabaseSchemaCommand";
import database from "models/resources/database";

type CdcSinkConfiguration = Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration;

export default class CdcSinkService {
    static async save(
        db: database | string,
        dto: CdcSinkConfiguration
    ): Promise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        return new saveCdcSinkCommand(db, dto).execute();
    }

    static async verify(
        db: database | string,
        connectionStringName: string
    ): Promise<Raven.Server.Documents.CdcSink.CdcSinkVerificationResult> {
        return new verifyCdcSinkSourceCommand(db, connectionStringName).execute();
    }

    static async test(
        db: database | string,
        dto: CdcSinkConfiguration,
        message: string
    ): Promise<Raven.Server.Documents.CdcSink.Test.TestCdcSinkScriptResult> {
        const payload: Raven.Server.Documents.CdcSink.Test.TestCdcSinkScript = {
            Configuration: dto,
            Message: message,
        };
        return new testCdcSinkCommand(db, payload).execute();
    }

    static async fetchSchema(
        db: database | string,
        sourceSqlDatabase: Raven.Server.SqlMigration.Model.SourceSqlDatabase
    ): Promise<Raven.Server.SqlMigration.Schema.DatabaseSchema> {
        return new fetchSqlDatabaseSchemaCommand(db, sourceSqlDatabase).execute();
    }
}
