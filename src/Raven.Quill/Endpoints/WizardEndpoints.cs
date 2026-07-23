using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Infrastructure;
using Raven.Quill.Wizard;

namespace Raven.Quill.Endpoints;

public static class WizardEndpoints
{
    private const string WizardSourceProbeName = "_wizard-source-probe";

    private const string DefaultIntentPrompt =
        "Propose a sensible RavenDB CDC document model from the discovered relational schema: " +
        "map each root/aggregate table to its own collection, embed parent-owned child rows " +
        "(1:N ownership) as nested arrays, and keep many-to-many or shared references as separate " +
        "collections linked by id. Use idiomatic collection names derived from the table names; " +
        "prefer a minimal, query-friendly shape over a literal table-per-collection mirror.";

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/setup").WithTags("setup").RequireAuthorization();
        group.MapPost("/connect", ConnectAsync)
            .WithName("setup.connect")
            .Accepts<ConnectRequest>("application/json")
            .Produces<ConnectResult>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/discover", DiscoverAsync)
            .WithName("setup.discover")
            .Accepts<DiscoverRequest>("application/json")
            .Produces<DiscoverResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/map", MapAsync)
            .WithName("setup.map")
            .Accepts<MapRequest>("application/json")
            .Produces<CdcSinkConfiguration>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/suggest/cdc", SuggestCdcAsync)
            .WithName("setup.suggestCdc")
            .Accepts<SuggestCdcRequest>("application/json")
            .Produces<SuggestCdcResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status422UnprocessableEntity);
        group.MapPost("/test-mapping", TestMappingAsync)
            .WithName("setup.testMapping")
            .Accepts<TestMappingRequest>("application/json")
            .Produces<TestMappingResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/provision", ProvisionAsync)
            .WithName("setup.provision")
            .WithDescription("Creates the app. The slug (also the app's database name, used in public " +
                             "embed URLs) derives from appName unless an explicit slug is supplied; either " +
                             "is normalized to lowercase ASCII alphanumerics with hyphens. Duplicate slug => 409.")
            .Accepts<ProvisionRequest>("application/json")
            .Produces<ProvisionResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status409Conflict);
    }

    private static async Task<IResult> ConnectAsync(
        ConnectRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (TryRejectInvalidRequest(body?.Provider, body?.ConnectionString, out var factoryName, out var error))
            return error;

        var sqlConnectionString = new SqlConnectionString
        {
            Name = WizardSourceProbeName,
            FactoryName = factoryName,
            ConnectionString = body!.ConnectionString,
        };
        await store.Maintenance.ForDatabase(store.Database).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString), ct);

        ConnectResult result;
        try
        {
            result = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new TestSqlConnectionOperation(factoryName, body.ConnectionString), ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Connect: test-connection threw");
            result = new ConnectResult();
            result.Errors.Add(new WizardError(ex.ToString()));
        }
        
        for (var i = 0; i < result.Errors.Count; i++)
            result.Errors[i] = WizardErrorFormatter.FormatConnectionError(result.Errors[i].Message);

        await PersistAsync(store, state =>
        {
            state.Provider = factoryName;
            state.LastVerifyResult = result;
            state.LastVerifyAt = DateTime.UtcNow;
        }, ct);

        return Results.Ok(result);
    }

    private static async Task<IResult> DiscoverAsync(
        DiscoverRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (TryRejectInvalidRequest(body?.Provider, body?.ConnectionString, out var factoryName, out var error))
            return error;

        var sqlConnectionString = new SqlConnectionString
        {
            Name = WizardSourceProbeName,
            FactoryName = factoryName,
            ConnectionString = body!.ConnectionString,
        };

        var schemas = body.Schemas?
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        CdcSinkSourceSchema schema;
        try
        {
            schema = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new GetCdcSinkSchemaOperation(sqlConnectionString, schemas is { Length: > 0 } ? schemas : null), ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Discover: schema enumeration threw");
            schema = new CdcSinkSourceSchema();
            schema.Errors.Add(ex.ToString());
        }

        await PersistAsync(store, state =>
        {
            state.Provider = factoryName;
            state.LastDiscoveredSchema = schema;
            state.LastDiscoverAt = DateTime.UtcNow;
        }, ct);

        return Results.Ok(DiscoverResponse.From(schema));
    }

    private static async Task<IResult> MapAsync(
        MapRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body required"));

        var cdcConfig = body.ToClientConfiguration();

        if (string.IsNullOrWhiteSpace(cdcConfig.Name))
            cdcConfig.Name = "wizard-cdc";
        if (string.IsNullOrWhiteSpace(cdcConfig.ConnectionStringName))
            cdcConfig.ConnectionStringName = WizardSourceProbeName;

        if (!cdcConfig.Validate(out var errors, validateName: true, validateConnection: false))
        {
            logger.LogInformation("Map: configuration rejected by Validate ({Count} errors)", errors.Count);
            return Results.BadRequest(new ApiErrorResponse(Errors: errors.ToArray()));
        }

        await PersistAsync(store, state =>
        {
            state.LastMapConfiguration = cdcConfig;
            state.LastMapAt = DateTime.UtcNow;
        }, ct);

        return Results.Ok(cdcConfig);
    }

    private static async Task<IResult> SuggestCdcAsync(
        SuggestCdcRequest body,
        IDocumentStore store,
        IAiHelperClient aiClient,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));

        var intentPrompt = string.IsNullOrWhiteSpace(body.IntentPrompt) ? DefaultIntentPrompt : body.IntentPrompt!;

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct);

        if (state?.LastDiscoveredSchema is null)
            return Results.BadRequest(new ApiErrorResponse("no discovered schema found; call /api/setup/discover first"));

        var result = await aiClient.SuggestCdcAsync(state.LastDiscoveredSchema, samples: null, intentPrompt, ct);

        if (result.Status != AiHelperStatus.Success)
            return Results.Ok(new SuggestCdcResponse(Configuration: null, result.Rationale, result.Status.ToString()));

        if (result.Configuration is null)
            return Results.UnprocessableEntity(new ApiErrorResponse("AI service returned a success status but no configuration"));

        if (!result.Configuration.Validate(out var errors, validateName: false, validateConnection: false))
        {
            logger.LogInformation("SuggestCdc: returned configuration failed validation ({Count} errors)", errors.Count);
            return Results.UnprocessableEntity(new ApiErrorResponse(Errors: errors.ToArray()));
        }

        return Results.Ok(new SuggestCdcResponse(result.Configuration, result.Rationale, result.Status.ToString()));
    }

    private static async Task<IResult> TestMappingAsync(
        TestMappingRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.SourceTableName))
            return Results.BadRequest(new ApiErrorResponse("sourceTableName is required"));

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct);

        if (state?.LastMapConfiguration is null)
            return Results.BadRequest(new ApiErrorResponse("no map configuration found; call /api/setup/map first"));

        var request = new TestCdcSinkMappingRequest
        {
            Configuration = state.LastMapConfiguration,
            SourceTableSchema = body.SourceTableSchema,
            SourceTableName = body.SourceTableName,
            RowSelector = TestCdcSinkRowSelector.First,
            Operation = TestCdcSinkOperation.Upsert,
            MaxRows = body.MaxRows ?? 50,
        };

        TestCdcSinkMappingResult result;
        try
        {
            result = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new TestCdcSinkMappingOperation(request), ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "TestMapping: SendAsync threw");
            result = new TestCdcSinkMappingResult();
            result.Errors.Add(ex.ToString());
        }

        return Results.Ok(TestMappingResponse.From(result));
    }

    private static async Task<IResult> ProvisionAsync(
        ProvisionRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.AppName))
            return Results.BadRequest(new ApiErrorResponse("appName is required"));

        var hasSlugOverride = string.IsNullOrWhiteSpace(body.Slug) == false;
        var slug = Slugifier.ToSlug(hasSlugOverride ? body.Slug : body.AppName);
        if (string.IsNullOrEmpty(slug))
            return Results.BadRequest(new ApiErrorResponse(hasSlugOverride
                ? $"slug '{body.Slug}' has no ASCII alphanumeric characters; cannot derive slug."
                : $"appName '{body.AppName}' has no ASCII alphanumeric characters; cannot derive slug."));

        if (slug.Length > Slugifier.MaxLength)
            return Results.BadRequest(new ApiErrorResponse(
                $"slug '{slug}' exceeds the maximum length of {Slugifier.MaxLength} characters"));

        if (string.Equals(slug, store.Database, StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest(new ApiErrorResponse($"slug '{slug}' is reserved"));

        CdcSinkConfiguration cdcConfig;
        using (var session = store.OpenAsyncSession(new global::Raven.Client.Documents.Session.SessionOptions { NoTracking = true }))
        {
            var state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct);
            if (state?.LastMapConfiguration is null)
                return Results.BadRequest(new ApiErrorResponse("no map configuration found; call /api/setup/map first"));

            cdcConfig = state.LastMapConfiguration;
        }

        bool created;
        try
        {
            // cluster-wide-atomic: this call IS the slug-uniqueness gate
            created = await RavenStoreFactory.EnsureDatabaseAsync(store, slug, ct: ct);
        }
        catch (ConcurrencyException)
        {
            return Results.Conflict(new ApiErrorResponse(DatabaseExistsMessage(slug)));
        }

        if (!created)
            return Results.Conflict(new ApiErrorResponse(DatabaseExistsMessage(slug)));

        var probes = await store.Maintenance.ForDatabase(store.Database).SendAsync(
            new GetConnectionStringsOperation(WizardSourceProbeName, ConnectionStringType.Sql), ct);

        if (probes.SqlConnectionStrings is null ||
            !probes.SqlConnectionStrings.TryGetValue(WizardSourceProbeName, out var probeCs))
        {
            return Results.BadRequest(new ApiErrorResponse(
                $"probe connection string '{WizardSourceProbeName}' is not registered on the config DB; call /api/setup/connect first."));
        }

        var transplantedCs = new SqlConnectionString
        {
            Name = cdcConfig.ConnectionStringName,
            FactoryName = probeCs.FactoryName,
            ConnectionString = probeCs.ConnectionString,
        };
        await store.Maintenance.ForDatabase(slug).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(transplantedCs), ct);

        cdcConfig.Name = $"{slug}-cdc";
        await store.Maintenance.ForDatabase(slug).SendAsync(new AddCdcSinkOperation(cdcConfig), ct);

        await AppDatabaseFeatures.ConfigureAsync(store, slug, ct);

        var app = new App
        {
            Slug = slug,
            AppName = body.AppName,
            Database = slug,
            CdcTaskName = cdcConfig.Name,
            CreatedAt = DateTime.UtcNow,
        };

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;
            // slug-keyed id (not HiLo): avoids the W6->W7 index-staleness race (C1/C2)
            await session.StoreAsync(app, id: $"apps/{slug}", ct);
            await session.SaveChangesAsync(ct);
        }

        // the wizard is done with the source credentials; the probe CS must not outlive it
        await store.Maintenance.ForDatabase(store.Database).SendAsync(
            new RemoveConnectionStringOperation<SqlConnectionString>(new SqlConnectionString { Name = WizardSourceProbeName }), ct);

        logger.LogInformation("Provisioned app slug={Slug} id={Id} cdcTask={CdcTaskName}",
            app.Slug, app.Id, app.CdcTaskName);

        return Results.Ok(new ProvisionResponse(app.Id!, app.Slug));
    }

    private static string DatabaseExistsMessage(string slug) =>
        $"database '{slug}' already exists; delete it in RavenDB Studio (or choose another name) and run the setup wizard again";

    private static bool TryRejectInvalidRequest(string? provider, string? connectionString, out string factoryName, out IResult error)
    {
        factoryName = string.Empty;

        if (string.IsNullOrWhiteSpace(provider) || string.IsNullOrWhiteSpace(connectionString))
        {
            error = Results.BadRequest(new ApiErrorResponse("provider and connectionString are required"));
            return true;
        }

        if (!SqlConnectionStringValidation.TryNormalizeCdcProvider(provider, out factoryName, out var providerError))
        {
            error = Results.BadRequest(providerError);
            return true;
        }

        error = default!;
        return false;
    }

    // one retry on OCC clash: absorbs a wizard-step double-click without livelocking
    private static async Task PersistAsync(
        IDocumentStore store,
        Action<WizardState> mutate,
        CancellationToken ct)
    {
        const int MaxAttempts = 2;
        for (var attempt = 1;; attempt++)
        {
            using var session = store.OpenAsyncSession();
            session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;

            var state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct)
                        ?? new WizardState();
            mutate(state);
            await session.StoreAsync(state, WizardState.DocumentId, ct);

            try
            {
                await session.SaveChangesAsync(ct);
                return;
            }
            catch (ConcurrencyException) when (attempt < MaxAttempts)
            {
            }
        }
    }

    internal sealed class WizardLogger;
}
