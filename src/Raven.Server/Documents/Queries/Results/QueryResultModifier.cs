using Raven.Client;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Queries.Results
{
    public class QueryResultModifier : JsBlittableBridge.IResultModifier
    {
        public static readonly QueryResultModifier Instance = new QueryResultModifier();

        public void Modify(JsHandle json)
        {
            using (var jsMetadata = json.GetProperty(Constants.Documents.Metadata.Key))
            {
                var engine = json.Engine;

                if (!jsMetadata.IsObject)
                {
                    using (var jsMetadataNew = engine.CreateObject())
                        jsMetadata.Set(jsMetadataNew);
                    json.SetProperty(Constants.Documents.Metadata.Key, jsMetadata.Clone(), throwOnError:false);
                }

                jsMetadata.SetProperty(Constants.Documents.Metadata.Projection, engine.CreateValue(true), throwOnError:false);
            }
        }
    }
}
