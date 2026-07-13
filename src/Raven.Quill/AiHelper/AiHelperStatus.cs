namespace Raven.Quill.AiHelper;

/// <summary>
/// Outcome of an AI-Helper call, surfaced to the dashboard. The first five values mirror
/// the internal service's <c>AiResponseStatus</c> (api.ravendb.net). <see cref="InternalError"/>
/// is the appliance-side fallback for transport and parse failures not covered by that set.
/// </summary>
public enum AiHelperStatus
{
    Success,
    InvalidCredentials,
    InvalidData,
    ConsentRequired,
    OutOfTokens,
    InternalError,
}
