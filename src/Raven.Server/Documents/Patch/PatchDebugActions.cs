using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    public class PatchDebugActions
    {
        public readonly DynamicJsonArray PutDocument = new DynamicJsonArray();
        public readonly DynamicJsonArray LoadDocument = new DynamicJsonArray();
        public readonly DynamicJsonArray DeleteDocument = new DynamicJsonArray();

        public DynamicJsonValue GetDebugActions()
        {
            return new DynamicJsonValue
            {
                [nameof(DeleteDocument)] = DeleteDocument,
                [nameof(PutDocument)] = PutDocument,
                [nameof(LoadDocument)] = LoadDocument
            };
        }
    }
}
