using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Infrastructure;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;

namespace Raven.AiAppliance.Endpoints;

/// Stage C.1 wizard backend endpoints. Reachability is enforced by
/// <c>ReadinessGateMiddleware</c> (returns 503 for non-bootstrap routes
/// while <c>IServerReady.IsReady</c> is false); handlers can assume the
/// config DB exists.
public static class WizardEndpoints
{
    /// Fixed-name connection string used by the wizard to probe a source DB
    /// before any per-app configuration exists. Lives on the config DB; gets
    /// overwritten on each Connect call.
    private const string WizardSourceProbeName = "_wizard-source-probe";

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/setup").WithTags("setup");
        group.MapPost("/connect", ConnectAsync)
            .WithName("setup.connect")
            .Accepts<ConnectRequest>("application/json")
            .Produces<ConnectResult>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/discover", DiscoverAsync)
            .WithName("setup.discover")
            .Accepts<ConnectRequest>("application/json")
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
        if (TryRejectInvalidRequest(body, out var factoryName, out var error))
            return error;

        var sqlConnectionString = new SqlConnectionString
        {
            Name             = WizardSourceProbeName,
            FactoryName      = factoryName,
            ConnectionString = body.ConnectionString,
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
            result.Errors.Add($"Connection test threw: {ex.Message}");
        }

        await PersistAsync(store, state =>
        {
            // Don't persist the raw ConnectionString — credentials are kept
            // only on the registered _wizard-source-probe SqlConnectionString
            // (one source of truth) to minimise exposure.
            state.Provider         = factoryName;
            state.LastVerifyResult = result;
            state.LastVerifyAt     = DateTime.UtcNow;
        }, ct);

        return Results.Ok(result);
    }

    private static async Task<IResult> DiscoverAsync(
        ConnectRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (TryRejectInvalidRequest(body, out var factoryName, out var error))
            return error;

        var sqlConnectionString = new SqlConnectionString
        {
            Name             = WizardSourceProbeName,
            FactoryName      = factoryName,
            ConnectionString = body.ConnectionString,
        };

        CdcSinkSourceSchema schema;
        try
        {
            schema = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new GetCdcSinkSchemaOperation(sqlConnectionString), ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Discover: schema enumeration threw");
            schema = new CdcSinkSourceSchema();
            schema.Errors.Add($"Discovery threw: {ex.Message}");
        }

        await PersistAsync(store, state =>
        {
            // ConnectionString deliberately not persisted — see Connect handler note.
            state.Provider             = factoryName;
            state.LastDiscoveredSchema = schema;
            state.LastDiscoverAt       = DateTime.UtcNow;
        }, ct);

        return Results.Ok(DiscoverResponse.From(schema));
    }

    /// <summary>
    /// W3 Map. Accepts a CDC mapping JSON (manual or import path),
    /// applies forgiving defaults for the wizard-context fields (Name +
    /// ConnectionStringName), validates it, and persists to wizard-state for
    /// W4 Test-mapping / W6 Provision to read back. No LLM path (AI-suggest)
    /// in this slice.
    /// </summary>
    private static async Task<IResult> MapAsync(
        MapRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body required"));

        var cdcConfig = body.ToClientConfiguration();

        // Forgiving defaults — caller supplies Tables, wizard fills the
        // scaffolding. Provision (W6) renames `Name` to the per-app value;
        // ConnectionStringName defaults to the probe Connect/Discover
        // already register on the config DB.
        if (string.IsNullOrWhiteSpace(cdcConfig.Name))
            cdcConfig.Name = "wizard-cdc";
        if (string.IsNullOrWhiteSpace(cdcConfig.ConnectionStringName))
            cdcConfig.ConnectionStringName = WizardSourceProbeName;

        // validateConnection: false — Map doesn't bind a real SqlConnectionString
        // to the config object. The probe is on the config DB; the per-app CS is
        // set up at Provision time.
        if (!cdcConfig.Validate(out var errors, validateName: true, validateConnection: false))
        {
            logger.LogInformation("Map: configuration rejected by Validate ({Count} errors)", errors.Count);
            return Results.BadRequest(new ApiErrorResponse(Errors: errors.ToArray()));
        }

        await PersistAsync(store, state =>
        {
            state.LastMapConfiguration = cdcConfig;
            state.LastMapAt            = DateTime.UtcNow;
        }, ct);

        return Results.Ok(cdcConfig);
    }

    /// <summary>
    /// AI-suggest counterpart to W3 Map. Gathers the discovered schema (from Discover),
    /// asks the internal AI service for a draft <see cref="CdcSinkConfiguration"/>,
    /// re-validates it, and returns it for the editable Review card. <b>Generate-only</b>: it does
    /// not persist; the admin edits and the existing <c>/api/setup/map</c> stays the single writer.
    /// Non-Success internal statuses (OutOfTokens / InvalidCredentials) are surfaced in the response
    /// <c>Status</c> with a null configuration rather than as an HTTP error.
    /// </summary>
    private static async Task<IResult> SuggestCdcAsync(
        SuggestCdcRequest body,
        IDocumentStore store,
        IAiHelperClient aiClient,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.IntentPrompt))
            return Results.BadRequest(new ApiErrorResponse("intentPrompt is required"));

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct);

        if (state?.LastDiscoveredSchema is null)
            return Results.BadRequest(new ApiErrorResponse("no discovered schema found; call /api/setup/discover first"));

        // Pass the discovered schema (CdcSinkSourceSchema, internal to Raven.Client) straight to the
        // client as object; the client serializes it via store conventions to the canonical wire shape.
        var result = await aiClient.SuggestCdcAsync(state.LastDiscoveredSchema, samples: null, body.IntentPrompt, ct);

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

    /// <summary>
    /// W4 Test-mapping. Reads wizard-state.LastMapConfiguration back, builds a
    /// TestCdcSinkMappingRequest with sane defaults, and forwards to the
    /// server's /admin/cdc-sink/test endpoint via TestCdcSinkMappingOperation
    /// (internal in Raven.Client; reachable here via InternalsVisibleTo). The
    /// server resolves the source-DB credentials by name from the registered
    /// _wizard-source-probe SqlConnectionString — no credentials re-sent.
    /// </summary>
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
            Configuration     = state.LastMapConfiguration,
            SourceTableSchema = body.SourceTableSchema,
            SourceTableName   = body.SourceTableName,
            RowSelector       = TestCdcSinkRowSelector.First,
            Operation         = TestCdcSinkOperation.Upsert,
            MaxRows           = body.MaxRows ?? 50,
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
            result.Errors.Add($"Test mapping threw: {ex.Message}");
        }

        return Results.Ok(TestMappingResponse.From(result));
    }

    /// <summary>
    /// W6 Provision. Creates the per-app RavenDB database, transplants the
    /// source SQL connection string from the wizard probe into it, installs
    /// the CDC Sink task with the wizard's stored Map configuration, and
    /// persists an <see cref="App"/> registry document on the config DB. The
    /// per-app database name equals the derived <c>slug</c>; RavenDB's
    /// cluster-wide-atomic <c>CreateDatabaseOperation</c> is the uniqueness
    /// gate (no separate compare-exchange needed). Best-effort on partial
    /// failures — operator cleans up orphans via Studio if any step after
    /// step 3 (per-app DB created) fails.
    /// </summary>
    private static async Task<IResult> ProvisionAsync(
        ProvisionRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.AppName))
            return Results.BadRequest(new ApiErrorResponse("appName is required"));

        var slug = Slugifier.ToSlug(body.AppName);
        if (string.IsNullOrEmpty(slug))
            return Results.BadRequest(new ApiErrorResponse(
                $"appName '{body.AppName}' has no ASCII alphanumeric characters; cannot derive slug."));

        // Read LastMapConfiguration NoTracking — we'll mutate Name in-place
        // before installing on the per-app DB, and the no-tracking option
        // keeps that mutation from drifting back into wizard-state on a
        // session SaveChanges. SessionOptions.Database left null so the
        // store's default (the config DB) resolves.
        CdcSinkConfiguration cdcConfig;
        using (var session = store.OpenAsyncSession(new global::Raven.Client.Documents.Session.SessionOptions { NoTracking = true }))
        {
            var state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct);
            if (state?.LastMapConfiguration is null)
                return Results.BadRequest(new ApiErrorResponse("no map configuration found; call /api/setup/map first"));

            cdcConfig = state.LastMapConfiguration;
        }

        // Create the per-app DB. CreateDatabaseOperation is cluster-wide-atomic
        // on the DB name, so this call IS the slug-uniqueness gate. A losing
        // racer (or a stale orphan from a partial-failure prior run) sees
        // false / ConcurrencyException -> 409.
        bool created;
        try
        {
            created = await RavenStoreFactory.EnsureDatabaseAsync(store, slug, ct);
        }
        catch (ConcurrencyException)
        {
            return Results.Conflict(new ApiErrorResponse($"database '{slug}' already exists"));
        }
        if (!created)
            return Results.Conflict(new ApiErrorResponse($"database '{slug}' already exists"));

        // Transplant the source SqlConnectionString from the config DB probe
        // to the per-app DB. The CDC task references the CS by name, so the
        // name on the per-app DB must match cdcConfig.ConnectionStringName
        // (W3 default is _wizard-source-probe). Credentials read fresh from
        // the registered probe — we never store them on the wizard-state doc.
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
            Name             = cdcConfig.ConnectionStringName,
            FactoryName      = probeCs.FactoryName,
            ConnectionString = probeCs.ConnectionString,
        };
        await store.Maintenance.ForDatabase(slug).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(transplantedCs), ct);

        // Install CDC Sink task on the per-app DB. Server-side AddCdcSinkOperation
        // auto-starts the initial load (see CdcSinkProcess.HandleInitialLoad).
        cdcConfig.Name = $"{slug}-cdc";
        await store.Maintenance.ForDatabase(slug).SendAsync(new AddCdcSinkOperation(cdcConfig), ct);

        // Register the App on the config DB. Id is auto-assigned via HiLo
        // (apps/1-A, apps/2-A, ...). OCC.Writes is harmless for inserts — HiLo
        // guarantees the id is fresh — but keeps the convention consistent with
        // PersistAsync.
        var app = new App
        {
            Slug        = slug,
            AppName     = body.AppName,
            Database    = slug,
            CdcTaskName = cdcConfig.Name,
            CreatedAt   = DateTime.UtcNow,
        };

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;
            // Slug-keyed id (not HiLo) so the App lookup in W7 / W8 / GetAsync
            // can use LoadAsync<App>($"apps/{slug}") instead of an index query.
            // Eliminates the index-staleness race between W6 Provision and
            // immediately-following W7/W8 calls in the wizard chain (the race
            // Copilot review #4361946757 C1/C2 flagged on PR #9).
            await session.StoreAsync(app, id: $"apps/{slug}", ct);
            await session.SaveChangesAsync(ct);
        }

        logger.LogInformation("Provisioned app slug={Slug} id={Id} cdcTask={CdcTaskName}",
            app.Slug, app.Id, app.CdcTaskName);

        return Results.Ok(new ProvisionResponse(app.Id!, app.Slug));
    }

    private static bool TryRejectInvalidRequest(ConnectRequest? body, out string factoryName, out IResult error)
    {
        factoryName = string.Empty;

        if (body is null || string.IsNullOrWhiteSpace(body.Provider) || string.IsNullOrWhiteSpace(body.ConnectionString))
        {
            error = Results.BadRequest(new ApiErrorResponse("provider and connectionString are required"));
            return true;
        }

        if (!SqlConnectionStringValidation.TryNormalizeCdcProvider(body.Provider, out factoryName, out var providerError))
        {
            error = Results.BadRequest(providerError);
            return true;
        }

        error = default!;
        return false;
    }

    /// <summary>
    /// Load-mutate-store the singleton <see cref="WizardState"/> doc with
    /// optimistic concurrency. A losing writer (operator double-click on a
    /// wizard step, or a slow Connect finishing after a fast Map) gets one
    /// retry — enough to absorb the realistic collision without livelocking
    /// on a pathological loop.
    /// </summary>
    private static async Task PersistAsync(
        IDocumentStore store,
        Action<WizardState> mutate,
        CancellationToken ct)
    {
        const int MaxAttempts = 2;
        for (var attempt = 1; ; attempt++)
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
                // Lost the race with a parallel writer; reload + reapply the
                // mutation against the latest revision. One retry is enough —
                // the only realistic collision is a double-click on the same
                // wizard step.
            }
        }
    }

    /// Logger category marker.
    internal sealed class WizardLogger;
}
