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
                [nameof(DeleteDocument)] = new DynamicJsonArray(DeleteDocument.Items),
                [nameof(PutDocument)] = new DynamicJsonArray(PutDocument.Items),
                [nameof(LoadDocument)] = new DynamicJsonArray(LoadDocument.Items)
            };
        }

        public void Clear()
        {
            PutDocument.Clear();
            LoadDocument.Clear();
            DeleteDocument.Clear();
        }
    }
}
