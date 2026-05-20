namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Wire-shape of the verifier result returned by the server-side
/// <c>POST /admin/cdc-sink/verify</c>. Local mirror of
/// <c>Raven.Server.Documents.CdcSink.CdcSinkVerificationResult</c>; the appliance
/// can't reference the server type directly. Once the server-side adds a
/// <c>VerifyCdcSinkOperation</c> + moves the result DTO into Raven.Client (the
/// follow-up flagged in the plan), this type goes away in favour of the client
/// version.
/// </summary>
public sealed class ConnectResult
{
    public bool Success { get; set; }
    public bool HasPermissionToSetup { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
}
