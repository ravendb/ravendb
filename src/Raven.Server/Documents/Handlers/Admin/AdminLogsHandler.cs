using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Config;
using Raven.Server.EventListener;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Admin
{
    public sealed class AdminLogsHandler : ServerRequestHandler
    {
        private void AssertClientVersionForLogsConfiguration()
        {
            if (RequestRouter.TryGetClientVersion(HttpContext, out var version) == false)
                return;

            if (version.Major > 6)
                return;

            if (version.Major == 6 && version.Minor >= 2)
                return;

            throw new InvalidOperationException("This endpoints requires 6.2 Client API or newer.");
        }

        [RavenAction("/admin/logs/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetConfiguration()
        {
            AssertClientVersionForLogsConfiguration();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var logsConfiguration = RavenLogManager.Instance.GetLogsConfiguration(Server);
                var auditLogsConfiguration = RavenLogManager.Instance.GetAuditLogsConfiguration(Server);
                var microsoftLogsConfiguration = RavenLogManager.Instance.GetMicrosoftLogsConfiguration(Server);
                var adminLogsConfiguration = RavenLogManager.Instance.GetAdminLogsConfiguration(Server);

                var djv = new DynamicJsonValue
                {
                    [nameof(GetLogsConfigurationResult.Logs)] = logsConfiguration?.ToJson(),
                    [nameof(GetLogsConfigurationResult.AuditLogs)] = auditLogsConfiguration?.ToJson(),
                    [nameof(GetLogsConfigurationResult.MicrosoftLogs)] = microsoftLogsConfiguration?.ToJson(),
                    [nameof(GetLogsConfigurationResult.AdminLogs)] = adminLogsConfiguration?.ToJson()
                };

                var json = context.ReadObject(djv, "logs/configuration");

                writer.WriteObject(json);
            }
        }

        [RavenAction("/admin/logs/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetConfiguration()
        {
            AssertClientVersionForLogsConfiguration();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "logs/configuration");

                var configuration = JsonDeserializationServer.Parameters.SetLogsConfigurationParameters(json);
                if (configuration.Persist)
                    AssertCanPersistConfiguration();

                RavenLogManager.Instance.ConfigureLogging(configuration);

                if (configuration.Persist)
                {
                    try
                    {
                        using var jsonFileModifier = SettingsJsonModifier.Create(context, ServerStore.Configuration.ConfigPath);

                        if (configuration.Logs != null)
                        {
                            jsonFileModifier.SetOrRemoveIfDefault(configuration.Logs.MinLevel, c => c.Logs.MinLevel);
                            jsonFileModifier.SetOrRemoveIfDefault(configuration.Logs.MaxLevel, c => c.Logs.MaxLevel);
                        }

                        if (configuration.MicrosoftLogs != null)
                        {
                            jsonFileModifier.SetOrRemoveIfDefault(configuration.MicrosoftLogs.MinLevel, c => c.Logs.MicrosoftMinLevel);
                        }

                        await jsonFileModifier.ExecuteAsync();
                    }
                    catch (Exception e)
                    {
                        throw new PersistConfigurationException("The log configuration was modified but couldn't be persisted. The configuration will be reverted on server restart.", e);
                    }
                }
            }

            NoContentStatus();
        }

        [RavenAction("/admin/logs/watch", "GET", AuthorizationStatus.Operator)]
        public async Task RegisterForLogs()
        {
            using (var socket = await HttpContext.WebSockets.AcceptWebSocketAsync())
                await AdminLogsTarget.RegisterAsync(socket, ServerStore.ServerShutdown);
        }

        [RavenAction("/admin/logs/download", "GET", AuthorizationStatus.Operator)]
        public async Task Download()
        {
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} - Node [{ServerStore.NodeTag}] - Logs.zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            HttpContext.Response.Headers["Content-Type"] = "application/zip";

            var adminLogsFileName = $"admin.logs.download.{Guid.NewGuid():N}";
            var adminLogsFilePath = ServerStore._env.Options.TempPath.Combine(adminLogsFileName);

            var startUtc = GetDateTimeQueryString("from", required: false);
            var endUtc = GetDateTimeQueryString("to", required: false);

            if (startUtc >= endUtc)
                throw new ArgumentException($"End Date '{endUtc:yyyy-MM-ddTHH:mm:ss.fffffff} UTC' must be greater than Start Date '{startUtc:yyyy-MM-ddTHH:mm:ss.fffffff} UTC'");

            await using (var stream = SafeFileStream.Create(adminLogsFilePath.FullPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
                       FileOptions.DeleteOnClose | FileOptions.SequentialScan))
            {
                using (var archive = new ZipArchive(stream, ZipArchiveMode.Create, true))
                {
                    bool isEmptyArchive = true;

                    foreach (var file in RavenLogManager.Instance.GetLogFiles(Server, startUtc, endUtc))
                    {
                        // TODO [ppekrol]
                        //// Skip this file if either the last write time or the creation time could not be determined
                        //if (LoggingSource.TryGetLastWriteTimeUtc(filePath, out var logLastWriteTimeUtc) == false ||
                        //    LoggingSource.TryGetCreationTimeUtc(filePath, out var logCreationTimeUtc) == false)
                        //    continue;

                        //bool isWithinDateRange =
                        //    // Check if the file was created before the end date.
                        //    (endUtc.HasValue == false || logCreationTimeUtc < endUtc.Value) &&
                        //    // Check if the file was last modified after the start date.
                        //    (startUtc.HasValue == false || logLastWriteTimeUtc > startUtc.Value);

                        //// Skip this file if it does not fall within the specified date range
                        //if (isWithinDateRange == false)
                        //    continue;

                        try
                        {
                            var entry = archive.CreateEntry(file.Name);
                            await using (var fs = File.Open(file.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                            {
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                await using (var entryStream = entry.Open())
                                {
                                    await fs.CopyToAsync(entryStream);
                                }

                                isEmptyArchive = false;
                            }
                        }
                        catch (Exception e)
                        {
                            await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, file.Name);
                        }
                    }

                    // Add an informational file to the archive if no log files match the specified date range,
                    // ensuring the user receives a non-empty archive with an explanation.
                    if (isEmptyArchive)
                    {
                        const string infoFileName = "No logs matched the date range.txt";

                        // Create a dummy entry in the zip file
                        var infoEntry = archive.CreateEntry(infoFileName);
                        await using var entryStream = infoEntry.Open();
                        await using var streamWriter = new StreamWriter(entryStream);

                        var formattedStartUtc = startUtc.HasValue ? startUtc.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffff 'UTC'") : "not specified";
                        var formattedEndUtc = endUtc.HasValue ? endUtc.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffff 'UTC'") : "not specified";

                        await streamWriter.WriteAsync(
                            $"No log files were found that matched the specified date range from '{formattedStartUtc}' to '{formattedEndUtc}'.");
                    }
                }

                stream.Position = 0;
                await stream.CopyToAsync(ResponseBodyStream());
            }
        }

        [RavenAction("/admin/event-listener/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetEventListenerConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = context.ReadObject(EventListenerToLog.Instance.ToJson(), "event-listener/configuration");
                    writer.WriteObject(json);
                }
            }
        }

        [RavenAction("/admin/event-listener/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetEventListenerConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "event-listener/configuration");

                var configuration = JsonDeserializationServer.EventListenerConfiguration(json);
                if (configuration.Persist)
                    AssertCanPersistConfiguration();

                EventListenerToLog.Instance.UpdateConfiguration(configuration);

                if (configuration.Persist)
                {
                    try
                    {
                        using var jsonFileModifier = SettingsJsonModifier.Create(context, ServerStore.Configuration.ConfigPath);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.EventListenerMode, x => x.DebugConfiguration.EventListenerMode);
                        jsonFileModifier.CollectionSetOrRemoveIfDefault(configuration.EventTypes, x => x.DebugConfiguration.EventTypes);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.MinimumDurationInMs, x => x.DebugConfiguration.MinimumDuration);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.AllocationsLoggingIntervalInMs, x => x.DebugConfiguration.AllocationsLoggingInterval);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.AllocationsLoggingCount, x => x.DebugConfiguration.AllocationsLoggingCount);
                        await jsonFileModifier.ExecuteAsync();
                    }
                    catch (Exception e)
                    {
                        throw new PersistConfigurationException(
                            "The event listener configuration was modified but couldn't be persistent. The configuration will be reverted on server restart.", e);
                    }
                }

                NoContentStatus();
            }
        }
    }
}
