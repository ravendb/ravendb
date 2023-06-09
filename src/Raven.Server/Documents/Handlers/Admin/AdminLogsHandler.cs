using System;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NCrontab.Advanced.Extensions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Utils.MicrosoftLogging;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Sparrow.Server.Platform.Posix;
using Voron.Util;

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
                    [nameof(GetLogsConfigurationResult.RetentionSize)] =
                        LoggingSource.Instance.RetentionSize == long.MaxValue ? null : (object)LoggingSource.Instance.RetentionSize,
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

            var from = GetDateTimeQueryString("from", required: false);
            var to = GetDateTimeQueryString("to", required: false);

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

                        var hasLogDateTime = LoggingSource.LogInfo.TryGetDate(filePath, out var logDateTime);
                        if (hasLogDateTime)
                        {
                            if (from != null && logDateTime < from)
                                continue;

                            if (to != null && logDateTime > to)
                                continue;
                        }

                        try
                        {
                            var entry = archive.CreateEntry(fileName);
                            if (hasLogDateTime)
                                entry.LastWriteTime = logDateTime;

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
                var respondBody = new DynamicJsonValue {["IsActive"] = provider.IsActive, ["Loggers"] = minLogLevelPerLogger};
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
                await provider.Configuration.ReadConfigurationAsync(RequestBodyStream(), context, reset);
                provider.ApplyConfiguration();
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

        private IDisposable AcquireLocksAndGetLoggers(out SwitchLogger generic, out SwitchLogger server)
        {
            var genericLogger = LoggingSource.Instance.LoggersHolder.Generic;
            var serverLogger = Server.Logger;
            Monitor.Enter(genericLogger);
            Monitor.Enter(serverLogger);

            generic = genericLogger;
            server = serverLogger;
            return new DisposableAction(() =>
            {
                Monitor.Exit(genericLogger);
                Monitor.Exit(serverLogger);
            });
        }

        [RavenAction("/admin/loggers", "GET", AuthorizationStatus.Operator)]
        public async Task GetAllLoggers()
        {
            using (AcquireLocksAndGetLoggers(out var generic, out var server))
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());
                
                var djv = new DynamicJsonValue
                {
                    ["LogMode"] = LoggingSource.Instance.LogAvailability.GetMode(),
                    ["Loggers"] = new DynamicJsonValue {[generic.Name] = generic.ToJson(), [server.Name] = server.ToJson()}
                };
                var json = context.ReadObject(djv, "logs/loggers");
                writer.WriteObject(json);
            }
        }
        
        [RavenAction("/admin/loggers/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetLoggingTogglingConfiguration()
        {
            using (AcquireLocksAndGetLoggers(out var generic, out var server))
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());

                var configuration = new Dictionary<string, LogMode>();
                SwitchLoggerConfigurationHelper.GetConfigurationFromRoot(generic, configuration);
                SwitchLoggerConfigurationHelper.GetConfigurationFromRoot(server, configuration);

                var djv = new DynamicJsonValue {[nameof(SwitchLoggerConfiguration.Loggers)] = configuration.ToJson()};
                var json = context.ReadObject(djv, "loggers/configuration");
                writer.WriteObject(json);
            }
        }
        
        [RavenAction("/admin/loggers", "POST", AuthorizationStatus.Operator)]
        public async Task SetLoggerMode()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var input = await ctx.ReadForMemoryAsync(RequestBodyStream(), "SwitchLoggerConfiguration");
                if (input.TryGet("Configuration", out BlittableJsonReaderObject json) == false)
                    ThrowRequiredPropertyNameInRequest("Configuration");
            
                var configuration = JsonDeserializationServer.SwitchLoggerConfiguration(json);
                using (AcquireLocksAndGetLoggers(out var generic, out var server))
                {
                    generic.ResetRecursively();
                    server.ResetRecursively();
                    
                    foreach (var (path, mode) in configuration.Loggers)
                    {
                        var first = SwitchLoggerConfigurationHelper.IterateSwitches(path).FirstOrDefault();
                        if(first.IsNullOrWhiteSpace())
                            throw new InvalidOperationException("Empty path is not allowed");

                        SwitchLogger loggerSwitch;
                        switch (first)
                        {
                            case "Server" :
                                loggerSwitch = server;
                                break;
                            case "Generic" :
                                loggerSwitch = generic;
                                break;
                            default:
                                continue;
                        }

                        SwitchLoggerConfigurationHelper.ApplyConfiguration(loggerSwitch, SwitchLoggerConfigurationHelper.IterateSwitches(path).Skip(1), mode);
                    }
                }
            }
        }

        internal class SwitchLoggerConfiguration
        {
            public Dictionary<string, LogMode> Loggers { get; set; }
        }
    }
}

