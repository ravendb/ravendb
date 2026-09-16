using Raven.Client.ServerWide.Operations.Certificates;

namespace Raven.Quill.Contracts;

public sealed record GenerateClientCertificateRequest(
    string Name,
    SecurityClearance Clearance,
    string? Password,
    Dictionary<string, DatabaseAccess> Permissions);
