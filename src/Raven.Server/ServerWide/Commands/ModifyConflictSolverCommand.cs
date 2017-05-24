using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ModifyConflictSolverCommand : UpdateDatabaseCommand
    {
        public ConflictSolver Solver;

        public ModifyConflictSolverCommand():base(null){}

        public ModifyConflictSolverCommand(string databaseName) : base(databaseName){}
        
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
