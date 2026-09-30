namespace Raven.Quill.Contracts;

public sealed record TokensByAppResponse(AppTokens[] Apps, int RefreshedMinutesAgo);

public sealed record AppTokens(string Slug, long Tokens);
