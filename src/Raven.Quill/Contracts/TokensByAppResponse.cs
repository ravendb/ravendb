namespace Raven.Quill.Contracts;

/// <summary>
/// Per-app token-usage breakdown — the prototype's <c>getTokensByApp()</c>. One
/// <see cref="AppTokens"/> row per app, sorted by tokens descending.
/// <paramref name="RefreshedMinutesAgo"/> is the UI's freshness hint; it is 0
/// because the totals are computed live on each request (no cached snapshot yet —
/// see RavenDB-26870).
/// </summary>
public sealed record TokensByAppResponse(AppTokens[] Apps, int RefreshedMinutesAgo);

/// <param name="Slug">The app slug.</param>
/// <param name="Tokens">All-time token usage summed across the app's conversations.</param>
public sealed record AppTokens(string Slug, long Tokens);
