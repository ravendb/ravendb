using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ModifyConflictSolverCommand : UpdateDatabaseCommand
    {
        public BlittableJsonReaderObject Value;

        public ModifyConflictSolverCommand():base(null){}

        public ModifyConflictSolverCommand(string databaseName) : base(databaseName){}
        
        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.ConflictSolverConfig = JsonDeserializationServer.ConflictSolver(Value);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Value)] = Value;
        }
    }
}
