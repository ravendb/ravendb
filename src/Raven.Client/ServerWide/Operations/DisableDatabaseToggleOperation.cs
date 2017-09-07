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
    public class DisableDatabaseToggleOperation : IServerOperation<DisableDatabaseToggleResult>
    {
        private readonly bool _disable;
        private readonly Parameters _parameters;

        public DisableDatabaseToggleOperation(string name, bool disable)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _disable = disable;
            _parameters = new Parameters
            {
                Names = new[] { name }
            };
        }

        public DisableDatabaseToggleOperation(Parameters parameters, bool disable)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.Names == null || parameters.Names.Length == 0)
                throw new ArgumentNullException(nameof(parameters.Names));

            _disable = disable;
            _parameters = parameters;
        }

        public RavenCommand<DisableDatabaseToggleResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DisableDatabaseToggleCommand(conventions, context, _parameters, _disable);
        }

        public class DisableDatabaseToggleCommand : RavenCommand<DisableDatabaseToggleResult>
        {
            private readonly bool _disable;
            private readonly BlittableJsonReaderObject _parameters;

            public DisableDatabaseToggleCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters, bool disable)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                _disable = disable;
                _parameters = EntityToBlittable.ConvertEntityToBlittable(parameters, conventions, context);
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

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
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
            public string[] Names { get; set; }
        }
    }
}
