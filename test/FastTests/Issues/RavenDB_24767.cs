using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Routing;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_24767 : RavenTestBase
    {
        public RavenDB_24767(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Studio | RavenTestCategory.BackupExportImport)]
        public async Task Can_Validate_Smuggler_options()
        {
            using (var store = GetDocumentStore())
            {
                var options = new DatabaseSmugglerOptionsServerSide(AuthorizationStatus.ValidUser)
                {
                    TransformScript = @"
                        this.Name = this.Name + '_transformed';
                    "
                };

                await store.Maintenance.SendAsync(new ValidateSmugglerOptionsOperation(options));
            }
        }

        private sealed class ValidateSmugglerOptionsOperation : IMaintenanceOperation
        {
            private readonly DatabaseSmugglerOptionsServerSide _options;

            public ValidateSmugglerOptionsOperation(DatabaseSmugglerOptionsServerSide options)
            {
                _options = options ?? throw new ArgumentNullException(nameof(options));
            }
            public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new ValidateSmugglerOptionsCommand(_options);
            }

            private sealed class ValidateSmugglerOptionsCommand : RavenCommand
            {
                private readonly DatabaseSmugglerOptionsServerSide _options;

                public ValidateSmugglerOptionsCommand(DatabaseSmugglerOptionsServerSide options)
                {
                    _options = options ?? throw new ArgumentNullException(nameof(options));
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/smuggler/validate-options";

                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx)).ConfigureAwait(false), DocumentConventions.Default)
                    };

                    return request;
                }
            }
        }
    }
}
