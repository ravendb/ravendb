using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Config;
using Raven.Server.Exceptions;
using Raven.Server.Utils.MicrosoftLogging;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminLogsHandler : ServerRequestHandler
    {
        [RavenAction("/admin/logs/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(GetLogsConfigurationResult.CurrentMode)] = LoggingSource.Instance.LogMode,
                    [nameof(GetLogsConfigurationResult.Mode)] = ServerStore.Configuration.Logs.Mode,
                    [nameof(GetLogsConfigurationResult.Path)] = ServerStore.Configuration.Logs.Path.FullPath,
                    [nameof(GetLogsConfigurationResult.UseUtcTime)] = ServerStore.Configuration.Logs.UseUtcTime,
                    [nameof(GetLogsConfigurationResult.RetentionTime)] = LoggingSource.Instance.RetentionTime,
                    [nameof(GetLogsConfigurationResult.RetentionSize)] = LoggingSource.Instance.RetentionSize == long.MaxValue ? null : (object)LoggingSource.Instance.RetentionSize,
                    [nameof(GetLogsConfigurationResult.Compress)] = LoggingSource.Instance.Compressing
                };

                var json = context.ReadObject(djv, "logs/configuration");

                writer.WriteObject(json);
            }
        }

        [RavenAction("/admin/logs/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "logs/configuration");

                var configuration = JsonDeserializationServer.Parameters.SetLogsConfigurationParameters(json);

                if (configuration.RetentionTime == null)
                    configuration.RetentionTime = ServerStore.Configuration.Logs.RetentionTime?.AsTimeSpan;

                LoggingSource.Instance.SetupLogMode(
                    configuration.Mode,
                    Server.Configuration.Logs.Path.FullPath,
                    configuration.RetentionTime,
                    configuration.RetentionSize?.GetValue(SizeUnit.Bytes),
                    configuration.Compress);

                if (configuration.Persist)
                {
                    try
                    {
                        using var jsonFileModifier = SettingsJsonModifier.Create(context, ServerStore.Configuration.ConfigPath);
                        jsonFileModifier.SetOrRemoveIfDefault(LoggingSource.Instance.LogMode, x => x.Logs.Mode);
                        long? retentionSize = LoggingSource.Instance.RetentionSize == long.MaxValue
                            ? null : new Size(LoggingSource.Instance.RetentionSize, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
                        jsonFileModifier.SetOrRemoveIfDefault(retentionSize, x => x.Logs.RetentionSize);
                        jsonFileModifier.SetOrRemoveIfDefault((int)LoggingSource.Instance.RetentionTime.TotalHours, x => x.Logs.RetentionTime);
                        jsonFileModifier.SetOrRemoveIfDefault(LoggingSource.Instance.Compressing, x => x.Logs.Compress);
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
            {
                var context = new LoggingSource.WebSocketContext();

                foreach (var filter in HttpContext.Request.Query["only"])
                {
                    context.Filter.Add(filter, true);
                }
                foreach (var filter in HttpContext.Request.Query["except"])
                {
                    context.Filter.Add(filter, false);
                }

                await LoggingSource.Instance.Register(socket, context, ServerStore.ServerShutdown);
            }
        }

        [RavenAction("/admin/logs/download", "GET", AuthorizationStatus.Operator)]
        public async Task Download()
        {
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} - Node [{ServerStore.NodeTag}] - Logs.zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            HttpContext.Response.Headers["Content-Type"] = "application/zip";

            var adminLogsFileName = $"admin.logs.download.{Guid.NewGuid():N}";
            var adminLogsFilePath = ServerStore._env.Options.DataPager.Options.TempPath.Combine(adminLogsFileName);

            var fromUtc = GetDateTimeQueryString("from", required: false);
            var toUtc = GetDateTimeQueryString("to", required: false);

            using (var stream = SafeFileStream.Create(adminLogsFilePath.FullPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
                       FileOptions.DeleteOnClose | FileOptions.SequentialScan))
            {
                using (var archive = new ZipArchive(stream, ZipArchiveMode.Create, true))
                {
                    foreach (var filePath in Directory.GetFiles(ServerStore.Configuration.Logs.Path.FullPath))
                    {
                        var fileName = Path.GetFileName(filePath);
                        if (fileName.EndsWith(LoggingSource.LogInfo.LogExtension, StringComparison.OrdinalIgnoreCase) == false &&
                            fileName.EndsWith(LoggingSource.LogInfo.FullCompressExtension, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        var hasLogDateTime = LoggingSource.LogInfo.TryGetDateUtc(filePath, out var logDateTimeUtc);
                        if (hasLogDateTime && fromUtc.HasValue && logDateTimeUtc < fromUtc)
                            continue;

                        try
                        {
                            var entry = archive.CreateEntry(fileName);
                            if (hasLogDateTime)
                                entry.LastWriteTime = logDateTimeUtc;

                            using (var fs = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                            {
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                await using (var entryStream = entry.Open())
                                {
                                    await fs.CopyToAsync(entryStream);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, fileName);
                        }

                        if (hasLogDateTime && toUtc.HasValue && logDateTimeUtc >= toUtc)
                            break;
                    }
                }

                stream.Position = 0;
                await stream.CopyToAsync(ResponseBodyStream());
            }
        }

        [RavenAction("/admin/logs/microsoft/loggers", "GET", AuthorizationStatus.Operator)]
        public async Task GetMicrosoftLoggers()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var djv = new DynamicJsonValue();
                var provider = Server.GetService<MicrosoftLoggingProvider>();
                foreach (var (name, minLogLevel) in provider.GetLoggers())
                {
                    djv[name] = minLogLevel;
                }
                var json = context.ReadObject(djv, "logs/configuration");
                writer.WriteObject(json);
            }
        }

        [RavenAction("/admin/logs/microsoft/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetMicrosoftConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var djv = new DynamicJsonValue();
                var provider = Server.GetService<MicrosoftLoggingProvider>();
                foreach (var (category, logLevel) in provider.Configuration)
                {
                    djv[category] = logLevel;
                }
                var json = context.ReadObject(djv, "logs/configuration");
                writer.WriteObject(json);
            }
        }

        [RavenAction("/admin/logs/microsoft/state", "GET", AuthorizationStatus.Operator)]
        public async Task GetMicrosoftLoggersState()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var provider = Server.GetService<MicrosoftLoggingProvider>();
                var minLogLevelPerLogger = new DynamicJsonValue();
                var respondBody = new DynamicJsonValue
                {
                    ["IsActive"] = provider.IsActive,
                    ["Loggers"] = minLogLevelPerLogger
                };
                foreach (var (category, logger) in provider.Loggers)
                {
                    minLogLevelPerLogger[category] = logger.MinLogLevel;
                }
                var json = context.ReadObject(respondBody, "logs/configuration");
                writer.WriteObject(json);
            }
        }

        [RavenAction("/admin/logs/microsoft/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetMicrosoftConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                bool reset = GetBoolValueQueryString("reset", required: false) ?? false;
                var provider = Server.GetService<MicrosoftLoggingProvider>();
                var parameters = await context.ReadForMemoryAsync(RequestBodyStream(), "logs/configuration");
                if (parameters.TryGet("Configuration", out BlittableJsonReaderObject microsoftConfig) == false)
                    throw new InvalidOperationException($"The request body doesn't contain required 'Configuration' property - {parameters}");

                provider.Configuration.ReadConfiguration(microsoftConfig, reset);
                provider.ApplyConfiguration();

                if (parameters.TryGet("Persist", out bool persist) && persist)
                {
                    try
                    {
                        using var microsoftConfigModifier = JsonConfigFileModifier.Create(context, ServerStore.Configuration.Logs.MicrosoftLogsConfigurationPath.FullPath, overwriteWholeFile: true);
                        foreach (var (category, logLevel) in Server.GetService<MicrosoftLoggingProvider>().Configuration)
                        {
                            microsoftConfigModifier.Modifications[category] = logLevel;
                        }
                        await microsoftConfigModifier.ExecuteAsync();

                        using var settingJsonConfigModifier = SettingsJsonModifier.Create(context, ServerStore.Configuration.ConfigPath);
                        settingJsonConfigModifier.SetOrRemoveIfDefault(false, x => x.Logs.DisableMicrosoftLogs);
                        await settingJsonConfigModifier.ExecuteAsync();
                    }
                    catch (Exception e)
                    {
                        throw new PersistConfigurationException("The Microsoft configuration was modified but couldn't be persisted. The configuration will be reverted on server restart.", e);
                    }
                }
            }

            NoContentStatus();
        }

        [RavenAction("/admin/logs/microsoft/enable", "POST", AuthorizationStatus.Operator)]
        public Task EnableMicrosoftLog()
        {
            var provider = Server.GetService<MicrosoftLoggingProvider>();
            provider.ApplyConfiguration();

            NoContentStatus();
            return Task.CompletedTask;
        }

        [RavenAction("/admin/logs/microsoft/disable", "POST", AuthorizationStatus.Operator)]
        public Task DisableMicrosoftLog()
        {
            var provider = Server.GetService<MicrosoftLoggingProvider>();
            provider.DisableLogging();

            NoContentStatus();
            return Task.CompletedTask;
        }
    }
}
