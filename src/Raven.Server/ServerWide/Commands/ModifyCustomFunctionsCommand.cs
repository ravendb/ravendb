using Raven.Client.ServerWide;
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

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.CustomFunctions = CustomFunctions;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(CustomFunctions)] = CustomFunctions;
        }
    }
}
