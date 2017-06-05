using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Controllers
{
    public class TransformersController : ClusterAwareRavenDbApiController
    {
        [HttpGet]
        [RavenRoute("transformers/{*id}")]
        [RavenRoute("databases/{databaseName}/transformers/{*id}")]
        public HttpResponseMessage TransformerGet(string id)
        {
            var transformer = id;
            if (string.IsNullOrEmpty(transformer) == false && transformer != "/")
            {
                var transformerDefinition = Database.Transformers.GetTransformerDefinition(transformer);
                if (transformerDefinition == null || transformerDefinition.Temporary)
                    return GetEmptyMessage(HttpStatusCode.NotFound);

                return GetMessageWithObject(new
                {
                    Transformer = transformerDefinition,
                });
            }
            return GetEmptyMessage();
        }

        [HttpGet]
        [RavenRoute("transformers")]
        [RavenRoute("databases/{databaseName}/transformers")]
        public HttpResponseMessage TransformerGet()
        {
            var namesOnlyString = GetQueryStringValue("namesOnly");
            bool namesOnly;
            RavenJArray transformers;
            if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
                transformers = Database.Transformers.GetTransformerNames(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));
            else
                transformers = Database.Transformers.GetTransformers(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));

            return GetMessageWithObject(transformers);
        }

        [HttpPut]
        [RavenRoute("transformers/{*id}")]
        [RavenRoute("databases/{databaseName}/transformers/{*id}")]
        public async Task<HttpResponseMessage> TransformersPut(string id)
        {
            var transformer = id;
            var data = await ReadJsonObjectAsync<TransformerDefinition>().ConfigureAwait(false);
            if (data == null || string.IsNullOrEmpty(data.TransformResults))
                return GetMessageWithString("Expected json document with 'TransformResults' property", HttpStatusCode.BadRequest);

            var replicationQueryString = GetQueryStringValue(Constants.IsReplicatedUrlParamName);
            var isReplication = !string.IsNullOrWhiteSpace(replicationQueryString) &&
                replicationQueryString.Equals("true", StringComparison.InvariantCultureIgnoreCase);

            try
            {
                var transformerName = Database.Transformers.PutTransform(transformer, data, isReplication);
                return GetMessageWithObject(new { Transformer = transformerName }, HttpStatusCode.Created);
            }
            catch (Exception ex)
            {
                return GetMessageWithObject(new
                {
                    ex.Message,
                    Error = ex.ToString()
                }, HttpStatusCode.BadRequest);
            }
        }

        [HttpPost]
        [RavenRoute("transformers/{*id}")]
        [RavenRoute("databases/{databaseName}/transformers/{*id}")]
        public Task<HttpResponseMessage> TransformersPost(string id)
        {
            var transformer = id;
            var lockModeStr = GetQueryStringValue("mode");

            TransformerLockMode transformerLockMode;
            if (Enum.TryParse(lockModeStr, out transformerLockMode) == false)
                return GetMessageWithStringAsTask("Cannot understand transformer lock mode: " + lockModeStr, HttpStatusCode.BadRequest);

            var transformerDefinition = Database.IndexDefinitionStorage.GetTransformerDefinition(transformer);
            if (transformerDefinition == null)
                return GetMessageWithStringAsTask("Cannot find transformer : " + transformer, HttpStatusCode.NotFound);

            transformerDefinition.LockMode = transformerLockMode;
            transformerDefinition.TransformerVersion = (transformerDefinition.TransformerVersion ?? 0) + 1;
            Database.IndexDefinitionStorage.UpdateTransformerDefinitionWithoutUpdatingCompiledTransformer(transformerDefinition);

            return GetEmptyMessageAsTask();
        }

        [HttpDelete]
        [RavenRoute("transformers/{*id}")]
        [RavenRoute("databases/{databaseName}/transformers/{*id}")]
        public HttpResponseMessage TransformersDelete(string id)
        {
            var isReplication = GetQueryStringValue(Constants.IsReplicatedUrlParamName);
            var transformerVersionAsString = GetQueryStringValue(Constants.TransformerVersion);
            int? transformerVersionAsInt = null;
            int transformerVersion;
            if (int.TryParse(transformerVersionAsString, out transformerVersion))
            {
                transformerVersionAsInt = transformerVersion;
            }

            if (Database.Transformers.DeleteTransform(id, transformerVersionAsInt) &&
                !String.IsNullOrWhiteSpace(isReplication) && isReplication.Equals("true", StringComparison.InvariantCultureIgnoreCase))
            {
                const string emptyFrom = "<no hostname>";
                var from = Uri.UnescapeDataString(GetQueryStringValue("from") ?? emptyFrom);
                Log.Info("received transformer deletion from replication (replicating transformer tombstone, received from {0})", from);
            }

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }
    }
}
