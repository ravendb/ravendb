using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ToggleDatabasesStateOperation : IServerOperation<DisableDatabaseToggleResult>
    {
        private readonly bool _disable;
        private readonly Parameters _parameters;

        public ToggleDatabasesStateOperation(string databaseName, bool disable)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            _disable = disable;
            _parameters = new Parameters
            {
                DatabaseNames = new[] { databaseName }
            };
        }

        public ToggleDatabasesStateOperation(string[] databaseNames, bool disable) 
            : this(new Parameters { DatabaseNames = databaseNames }, disable)
        {
        }

        public ToggleDatabasesStateOperation(Parameters parameters, bool disable)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.DatabaseNames == null || parameters.DatabaseNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.DatabaseNames));

            _disable = disable;
            _parameters = parameters;
        }

        public RavenCommand<DisableDatabaseToggleResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ToggleDatabaseStateCommand(conventions, context, _parameters, _disable);
        }

        private class ToggleDatabaseStateCommand : RavenCommand<DisableDatabaseToggleResult>
        {
            private readonly bool _disable;
            private readonly BlittableJsonReaderObject _parameters;

            public ToggleDatabaseStateCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters, bool disable)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                _disable = disable;
                _parameters = EntityToBlittable.ConvertCommandToBlittable(parameters,context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var toggle = _disable ? "disable" : "enable";
                url = $"{node.Url}/admin/databases/{toggle}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _parameters);
                    })
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null ||
                    response.TryGet("Status", out BlittableJsonReaderArray databases) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }

                var resultObject = databases[0] as BlittableJsonReaderObject;
                Result = JsonDeserializationClient.DisableResourceToggleResult(resultObject);
            }

            public override bool IsReadRequest => false;
        }

        public class Parameters
        {
            public string[] DatabaseNames { get; set; }
        }
    }
}
