using System;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Wizard;

namespace Raven.Quill.Contracts;

public sealed record AppResponse(
    string Id,
    string Slug,
    string Name,
    string Database,
    string CdcTaskName,
    DateTime CreatedAt)
{
    internal static AppResponse From(App app) => new(
        app.Id ?? AppLookup.DocumentIdFor(app.Slug),
        app.Slug,
        app.AppName,
        app.Database,
        app.CdcTaskName,
        app.CreatedAt);
}
