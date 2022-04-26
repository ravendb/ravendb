using Raven.Client;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Queries.Results
{
    public class QueryResultModifier : IResultModifier
    {
        public static readonly QueryResultModifier Instance = new QueryResultModifier();

        public void Modify<T>(T json, IJsEngineHandle<T> engine) where T : struct, IJsHandle<T>
        {
            using (var jsMetadata = json.GetProperty(Constants.Documents.Metadata.Key))
            {
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
