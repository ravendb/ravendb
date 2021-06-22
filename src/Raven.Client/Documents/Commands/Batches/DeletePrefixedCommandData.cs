using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class DeletePrefixedCommandData : DeleteCommandData
    {
        public DeletePrefixedCommandData(string prefix) : base(prefix, null)
        {
        }
        
        private bool IdPrefixed => true;

        public override DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var result = base.ToJson(conventions, context);

            result[nameof(IdPrefixed)] = IdPrefixed;

            return result;
        }
    }
}
