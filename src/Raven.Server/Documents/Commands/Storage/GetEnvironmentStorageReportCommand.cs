using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Commands.Storage;

internal class GetEnvironmentStorageReportCommand : RavenCommand
{
    private readonly string _name;
    private readonly StorageEnvironmentWithType.StorageEnvironmentType _type;
    private readonly bool _details;

    public GetEnvironmentStorageReportCommand([NotNull] string name, [NotNull] StorageEnvironmentWithType.StorageEnvironmentType type, bool details, string nodeTag)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _type = type;
        _details = details;
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/debug/storage/environment/report?name={Uri.EscapeDataString(_name)}&type={Uri.EscapeDataString(_type.ToString())}&details={_details}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }
}
