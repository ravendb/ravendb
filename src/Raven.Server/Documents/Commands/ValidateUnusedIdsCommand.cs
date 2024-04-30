using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands;

internal sealed class ValidateUnusedIdsCommand : RavenCommand
{
    private readonly Parameters _parameters;

    internal ValidateUnusedIdsCommand(Parameters parameters)
    {
        if(parameters == null)
            throw new ArgumentNullException("'ValidateUnusedIdsCommand' parameters cannot be null");

        _parameters = parameters;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/validate-unused-ids";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            Content = new BlittableJsonContent(
                async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx))
                    .ConfigureAwait(false), DocumentConventions.Default)
        };
    }

    internal sealed class Parameters
    {
        public HashSet<string> DatabaseIds { get; set; }
    }
}

