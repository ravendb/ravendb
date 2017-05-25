using Raven.Client.Server;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ModifyConflictSolverCommand : UpdateDatabaseCommand
    {
        public BlittableJsonReaderObject Solver;

        public ModifyConflictSolverCommand():base(null){}

        public ModifyConflictSolverCommand(string databaseName) : base(databaseName){}
        
        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.ConflictSolverConfig = JsonDeserializationRachis<ConflictSolver>.Deserialize(Solver);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(ConflictSolver)] = Solver;
        }
    }
}
