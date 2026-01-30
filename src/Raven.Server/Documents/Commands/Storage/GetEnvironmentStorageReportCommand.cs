using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Commands.Storage;

internal sealed class GetEnvironmentStorageReportCommand : RavenCommand
{
    private readonly string _name;
    private readonly StorageEnvironmentWithType.StorageEnvironmentType _type;
    private readonly bool _details;
    private readonly bool _flat;

    public GetEnvironmentStorageReportCommand([NotNull] string name, [NotNull] StorageEnvironmentWithType.StorageEnvironmentType type, bool details, string nodeTag, bool flat)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _type = type;
        _details = details;
        _flat = flat;
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/debug/storage/environment/report?name={Uri.EscapeDataString(_name)}&type={Uri.EscapeDataString(_type.ToString())}&details={_details}&flat={_flat}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }
}
