using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ModifyConflictSolverCommand : UpdateDatabaseCommand
    {
        public string DatabaseResolverId;
        public BlittableJsonReaderObject ResolveByCollection;
        public bool ResolveToLatest;

        public ModifyConflictSolverCommand():base(null){}

        public ModifyConflictSolverCommand(string databaseName) : base(databaseName){}
        
        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.ConflictSolverConfig.DatabaseResolverId = DatabaseResolverId;
            record.ConflictSolverConfig.ResolveToLatest = ResolveToLatest;

            record.ConflictSolverConfig.ResolveByCollection = new Dictionary<string, ScriptResolver>();
            if (ResolveByCollection != null)
            {
                foreach (var propertyName in ResolveByCollection.GetPropertyNames())
                {
                    var script = ResolveByCollection[propertyName] as BlittableJsonReaderObject;
                    record.ConflictSolverConfig.ResolveByCollection.Add(
                        propertyName,
                        JsonDeserializationRachis<ScriptResolver>.Deserialize(script));
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(DatabaseResolverId)] = DatabaseResolverId;
            json[nameof(ResolveToLatest)] = ResolveToLatest;
            json[nameof(ResolveByCollection)] = ResolveByCollection;
        }
    }
}
