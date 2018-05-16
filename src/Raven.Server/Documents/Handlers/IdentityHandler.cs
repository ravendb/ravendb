using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class IdentityHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/identity/next", "POST", AuthorizationStatus.ValidUser)]
        public async Task NextIdentityFor()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (name[name.Length - 1] != '|')
                name += '|';

            var (_, _, newIdentityValue) = await Database.ServerStore.GenerateClusterIdentityAsync(name, Database.Name);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("NewIdentityValue");
                writer.WriteInteger(newIdentityValue);

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/identity/seed", "POST", AuthorizationStatus.ValidUser)]
        public async Task SeedIdentityFor()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var value = GetLongQueryString("value", true);
            var forced = GetBoolValueQueryString("force", false)??false;
            if (value == null)
                throw new ArgumentException("Query string value 'value' must have a non empty value");

            if (name[name.Length - 1] != '|')
                name += '|';

            var newIdentityValue = await Database.ServerStore.UpdateClusterIdentityAsync(name, Database.Name, value.Value, forced);
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("NewSeedValue");
                writer.WriteInteger(newIdentityValue);

                writer.WriteEndObject();
            }
        }
    }
}
