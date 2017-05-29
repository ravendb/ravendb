using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    class ModifyCustomFunctionsCommand : UpdateDatabaseCommand
    {
        public string CustomFunctions { get; set; }
        public ModifyCustomFunctionsCommand() : base(null)
        {
        }

        public ModifyCustomFunctionsCommand(string databaseName) : base(databaseName)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.CustomFunctions = CustomFunctions;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(CustomFunctions)] = CustomFunctions;
        }
    }
}
