using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ClusterBatchCommand : CommandBase
    {
        public List<CommandBase> CommandsList;

        public ClusterBatchCommand()
        {
        }
        
        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            var dja = new DynamicJsonArray();
            foreach (var command in CommandsList){
                dja.Add(command.ToJson(context));
            }
            djv[nameof(CommandsList)] = dja;

            return djv;
        }
    }
}
