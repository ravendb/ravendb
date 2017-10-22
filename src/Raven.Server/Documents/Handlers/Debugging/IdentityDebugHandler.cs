using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class IdentityDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/identities", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task GetIdentities()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var first = true;
                    foreach (var identity in Database.ServerStore.Cluster.ReadIdentities(context, Database.Name, start, pageSize))
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;
                        writer.WritePropertyName(identity.Prefix);
                        writer.WriteInteger(identity.Value);
                    }

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }


        [RavenAction("/databases/*/identity/next", "GET", AuthorizationStatus.ValidUser)]
        public async Task NextIdentityFor()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (name[name.Length - 1] != '|')
                name += '|';
            
            var (_, _, id) = await Database.ServerStore.GenerateClusterIdentityAsync(name, Database.Name);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Index");
                writer.WriteInteger(id);

                writer.WriteEndObject();
            }            
        }

        [RavenAction("/databases/*/identity/seed", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task SeedIdentityFor()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var value = GetLongQueryString("value", true);
            if (value == null)
                throw new ArgumentException("Query string value 'value' must have a non empty value");

            if (name[name.Length - 1] != '|')
                name += '|';

            var index = await Database.ServerStore.UpdateClusterIdentityAsync(name, Database.Name, value.Value);
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Index");
                writer.WriteInteger(index);

                writer.WriteEndObject();
            }
        }
    }
}
