using System;
using Raven.AiAppliance.Wizard;

namespace Raven.AiAppliance.Contracts;

public sealed record AppResponse(
    string Id,
    string Slug,
    string Name,
    string Database,
    string CdcTaskName,
    DateTime CreatedAt)
{
    internal static AppResponse From(App app) => new(
        app.Id ?? $"apps/{app.Slug}",
        app.Slug,
        app.AppName,
        app.Database,
        app.CdcTaskName,
        app.CreatedAt);
}
