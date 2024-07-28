using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ModifyConflictSolverCommand : UpdateDatabaseCommand
    {
        public ConflictSolver Solver;

        public ModifyConflictSolverCommand()
        { }

        public ModifyConflictSolverCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId){}
        
        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.ConflictSolverConfig = Solver;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Solver)] = Solver.ToJson();
        }
    }
}
