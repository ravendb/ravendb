using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using JetBrains.Annotations;
using NLog;
using NLog.Common;
using NLog.Config;
using NLog.Filters;
using NLog.Targets;
using NLog.Targets.Wrappers;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
#if !RVN
using System.Diagnostics;
using System.Runtime.Loader;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static.NuGet;
using Raven.Server.Documents.Sharding;
#endif
using Sparrow;
using Sparrow.Global;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using AsyncHelpers = Raven.Client.Util.AsyncHelpers;
using LogLevel = NLog.LogLevel;
using Size = Sparrow.Size;

namespace Raven.Server.Logging;

internal static class RavenLogManagerServerExtensions
{
    private static readonly NullTarget NullTarget = new(nameof(NullTarget));

    private static readonly ConcurrentDictionary<string, Assembly> LoadedAssemblies = new(StringComparer.OrdinalIgnoreCase);

    internal static LoggingRule DefaultRule;

    private static LoggingRule DefaultAuditRule;

    private static LoggingRule SystemRule = new()
    {
        RuleName = Constants.Logging.Names.SystemRuleName,
        FinalMinLevel = LogLevel.Warn,
        LoggerNamePattern = "System.*",
        Targets = { NullTarget }
    };

    private static LoggingRule MicrosoftRule = new()
    {
        RuleName = Constants.Logging.Names.MicrosoftRuleName,
        FinalMinLevel = LogLevel.Warn,
        LoggerNamePattern = "Microsoft.*",
        Targets = { NullTarget }
    };

#if !RVN
    internal static readonly LoggingRule AdminLogsRule = new()
    {
        RuleName = Constants.Logging.Names.AdminLogsRuleName,
        LoggerNamePattern = "*",
        Targets = { new AsyncTargetWrapper(AdminLogsTarget.Instance) { QueueLimit = 128, OverflowAction = AsyncTargetWrapperOverflowAction.Discard } }
    };

    internal static readonly LoggingRule PipeRule = new()
    {
        RuleName = Constants.Logging.Names.PipeRuleName,
        LoggerNamePattern = "*",
        Targets = { StreamTarget.Instance }
    };
#endif

    internal static readonly LoggingRule ConsoleRule = new()
    {
        RuleName = Constants.Logging.Names.ConsoleRuleName,
        LoggerNamePattern = "*",
        Targets = {
            new ConsoleTarget
            {
                DetectConsoleAvailable = true,
                Layout = Constants.Logging.DefaultLayout,
            }
        }
    };

#if !RVN
    private static readonly ConcurrentDictionary<string, RavenAuditLogger> AuditLoggers = new(StringComparer.OrdinalIgnoreCase);
#endif

    public static RavenLogger GetLoggerForCluster<T>(this RavenLogManager logManager, LoggingComponent component = null) => GetLoggerForCluster(logManager, typeof(T), component);

    public static RavenLogger GetLoggerForCluster(this RavenLogManager logManager, Type type, LoggingComponent component = null)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return GetLoggerForResourceInternal(logManager, type.FullName, LoggingResource.Cluster, component);
    }

    public static RavenLogger GetLoggerForServer<T>(this RavenLogManager logManager, LoggingComponent component = null) => GetLoggerForServer(logManager, typeof(T), component);

    public static RavenLogger GetLoggerForServer(this RavenLogManager logManager, Type type, LoggingComponent component = null)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return GetLoggerForResourceInternal(logManager, type.FullName, LoggingResource.Server, component);
    }

#if !RVN
    public static RavenLogger GetLoggerForDatabase<T>(this RavenLogManager logManager, [NotNull] DocumentDatabase database)
    {
        if (database == null)
            throw new ArgumentNullException(nameof(database));

        return GetLoggerForDatabase<T>(logManager, database.Name);
    }

    public static RavenLogger GetLoggerForDatabase(this RavenLogManager logManager, Type type, DocumentDatabase database)
    {
        if (database == null)
            throw new ArgumentNullException(nameof(database));

        return GetLoggerForDatabase(logManager, type, database.Name);
    }

    public static RavenLogger GetLoggerForDatabase<T>(this RavenLogManager logManager, [NotNull] ShardedDatabaseContext databaseContext)
    {
        if (databaseContext == null)
            throw new ArgumentNullException(nameof(databaseContext));

        return GetLoggerForDatabase<T>(logManager, databaseContext.DatabaseName);
    }

    public static RavenLogger GetLoggerForDatabase(this RavenLogManager logManager, Type type, ShardedDatabaseContext databaseContext)
    {
        if (databaseContext == null)
            throw new ArgumentNullException(nameof(databaseContext));

        return GetLoggerForDatabase(logManager, type, databaseContext.DatabaseName);
    }
#endif

    public static RavenLogger GetLoggerForDatabase<T>(this RavenLogManager logManager, string databaseName)
    {
        if (databaseName == null)
            throw new ArgumentNullException(nameof(databaseName));

        return GetLoggerForDatabase(logManager, typeof(T), databaseName);
    }

    public static RavenLogger GetLoggerForDatabase(this RavenLogManager logManager, Type type, string databaseName)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));
        if (databaseName == null)
            throw new ArgumentNullException(nameof(databaseName));

        return GetLoggerForDatabaseInternal(logManager, type.FullName, databaseName);
    }

#if !RVN
    public static RavenLogger GetLoggerForIndex<T>(this RavenLogManager logManager, [NotNull] Raven.Server.Documents.Indexes.Index index)
    {
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        return GetLoggerForIndex(logManager, typeof(T), index);
    }

    public static RavenLogger GetLoggerForIndex(this RavenLogManager logManager, [NotNull] Type type, [NotNull] Raven.Server.Documents.Indexes.Index index)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        return GetLoggerForIndexInternal(logManager, type.FullName, index.DocumentDatabase.Name, index.Name);
    }

    public static RavenAuditLogger GetAuditLoggerForServer(this RavenLogManager logManager)
    {
        return AuditLoggers.GetOrAdd(LoggingResource.Server.ToString(), r =>
        {
            var logger = LogManager.GetLogger("Audit")
                .WithProperty(Constants.Logging.Properties.Resource, r);

            return new RavenAuditLogger(logger);
        });
    }

    public static RavenAuditLogger GetAuditLoggerForDatabase(this RavenLogManager logManager, [NotNull] string databaseName)
    {
        if (databaseName == null)
            throw new ArgumentNullException(nameof(databaseName));

        return AuditLoggers.GetOrAdd(databaseName, r =>
        {
            var logger = LogManager.GetLogger("Audit")
                .WithProperty(Constants.Logging.Properties.Resource, r);

            return new RavenAuditLogger(logger);
        });
    }
#endif

    private static RavenLogger GetLoggerForResourceInternal(RavenLogManager logManager, string name, LoggingResource resource, LoggingComponent component)
    {
        return new RavenLogger(LogManager.GetLogger(name)
            .WithProperty(Constants.Logging.Properties.Resource, resource)
            .WithProperty(Constants.Logging.Properties.Component, component?.ToString()));
    }

    private static RavenLogger GetLoggerForDatabaseInternal(RavenLogManager logManager, string name, string databaseName) =>
        new RavenLogger(LogManager.GetLogger(name)
            .WithProperty(Constants.Logging.Properties.Resource, databaseName));

    private static RavenLogger GetLoggerForIndexInternal(RavenLogManager logManager, string name, string databaseName, string indexName) =>
        new RavenLogger(LogManager.GetLogger(name)
            .WithProperty(Constants.Logging.Properties.Resource, databaseName)
            .WithProperty(Constants.Logging.Properties.Component, indexName));

#if !RVN
    public static void ConfigureLogging(this RavenLogManager logManager, [NotNull] SetLogsConfigurationOperation.Parameters parameters)
    {
        if (parameters == null)
            throw new ArgumentNullException(nameof(parameters));

        if (parameters.MicrosoftLogs != null)
        {
            AssertNotFileConfig();

            SystemRule.FinalMinLevel = MicrosoftRule.FinalMinLevel = parameters.MicrosoftLogs.MinLevel.ToNLogFinalMinLogLevel();
        }

        if (parameters.Logs != null)
        {
            AssertNotFileConfig();

            DefaultRule.SetLoggingLevels(parameters.Logs.MinLevel.ToNLogLogLevel(), parameters.Logs.MaxLevel.ToNLogLogLevel());
            DefaultRule.FilterDefaultAction = parameters.Logs.LogFilterDefaultAction.ToNLogFilterResult();

            ApplyFilters(parameters.Logs.Filters, DefaultRule);
        }

        if (parameters.AdminLogs != null)
        {
            AdminLogsRule.SetLoggingLevels(parameters.AdminLogs.MinLevel.ToNLogLogLevel(), parameters.AdminLogs.MaxLevel.ToNLogLogLevel());
            AdminLogsRule.FilterDefaultAction = parameters.AdminLogs.LogFilterDefaultAction.ToNLogFilterResult();

            ApplyFilters(parameters.AdminLogs.Filters, AdminLogsRule);
        }

        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

        return;

        static void ApplyFilters(List<LogFilter> filters, LoggingRule rule)
        {
            rule.Filters.Clear();

            if (filters == null || filters.Count == 0)
                return;

            foreach (var filter in filters)
                rule.Filters.Add(new RavenConditionBasedFilter(filter));
        }
    }
#endif

    public static void ConfigureLogging(this RavenLogManager logManager, RavenConfiguration configuration)
    {
        LogManager.ThrowExceptions = configuration.Logs.ThrowExceptions;

        ConsoleRule.DisableLoggingForLevels(LogLevel.Trace, LogLevel.Fatal);
#if !RVN
        PipeRule.DisableLoggingForLevels(LogLevel.Trace, LogLevel.Fatal);
#endif
        if (TryConfigureLoggingFromFile(configuration))
            return;

        SystemRule.FinalMinLevel = MicrosoftRule.FinalMinLevel = configuration.Logs.MicrosoftMinLevel.ToNLogFinalMinLogLevel();

        var minLevel = configuration.Logs.MinLevel;
        var maxLevel = configuration.Logs.MaxLevel;
        var archiveAboveSize = configuration.Logs.ArchiveAboveSize;
        var enableArchiveFileCompression = configuration.Logs.EnableArchiveFileCompression;
        var maxArchiveDays = configuration.Logs.MaxArchiveDays;
        var maxArchiveFiles = configuration.Logs.MaxArchiveFiles;

#if !RVN
        if (TryGetLegacyLogLevel(configuration, out var legacyMinLevel, out var legacyMaxLevel))
        {
            minLevel = legacyMinLevel;
            maxLevel = legacyMaxLevel;
        }

        if (TryGetLegacyArchiveAboveSize(configuration, out var legacyArchiveAboveSize))
            archiveAboveSize = legacyArchiveAboveSize;

        if (TryGetLegacyMaxArchiveDays(configuration, out var legacyMaxArchiveDays))
            maxArchiveDays = legacyMaxArchiveDays;
#endif

        ConfigureInternalLogging();

        var fileTarget = new FileTarget
        {
            Name = nameof(FileTarget),
            CreateDirs = true,
            FileName = configuration.Logs.Path.Combine("${shortdate}.log").FullPath,
            ArchiveNumbering = ArchiveNumberingMode.DateAndSequence,
            Header = Constants.Logging.DefaultHeaderAndFooterLayout,
            Layout = Constants.Logging.DefaultLayout,
            Footer = Constants.Logging.DefaultHeaderAndFooterLayout,
            ConcurrentWrites = false,
            WriteFooterOnArchivingOnly = true,
            ArchiveAboveSize = archiveAboveSize.GetValue(SizeUnit.Bytes),
            EnableArchiveFileCompression = enableArchiveFileCompression,
        };

        if (maxArchiveDays.HasValue)
            fileTarget.MaxArchiveDays = maxArchiveDays.Value;

        if (maxArchiveFiles.HasValue)
            fileTarget.MaxArchiveFiles = maxArchiveFiles.Value;

        var fileTargetAsyncWrapper = new AsyncTargetWrapper(nameof(AsyncTargetWrapper), fileTarget);

        DefaultRule = new LoggingRule("*", minLevel.ToNLogLogLevel(), maxLevel.ToNLogLogLevel(), fileTargetAsyncWrapper)
        {
            RuleName = Constants.Logging.Names.DefaultRuleName
        };

        DefaultAuditRule = new LoggingRule("Audit", LogLevel.Info, LogLevel.Info, NullTarget)
        {
            RuleName = Constants.Logging.Names.DefaultAuditRuleName,
            Final = true
        };

        var config = new LoggingConfiguration();

        config.AddRule(SystemRule);
        config.AddRule(MicrosoftRule);
        config.AddRule(DefaultAuditRule);
        config.AddRule(DefaultRule);

        LogManager.Setup(x => x.LoadConfiguration(config));
        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

#if !RVN
        static bool TryGetLegacyLogLevel(RavenConfiguration configuration, out Sparrow.Logging.LogLevel legacyMinLevel, out Sparrow.Logging.LogLevel legacyMaxLevel)
        {
            var logsMode = configuration.GetSetting("Logs.Mode");
            switch (logsMode)
            {

                case "None":
                    legacyMinLevel = Sparrow.Logging.LogLevel.Off;
                    legacyMaxLevel = Sparrow.Logging.LogLevel.Off;
                    return true;
                case "Information":
                    legacyMinLevel = Sparrow.Logging.LogLevel.Debug;
                    legacyMaxLevel = Sparrow.Logging.LogLevel.Fatal;
                    return true;
                case "Operations":
                    legacyMinLevel = Sparrow.Logging.LogLevel.Info;
                    legacyMaxLevel = Sparrow.Logging.LogLevel.Fatal;
                    return true;
            }

            legacyMinLevel = Sparrow.Logging.LogLevel.Off;
            legacyMaxLevel = Sparrow.Logging.LogLevel.Off;
            return false;
        }
#endif
        static bool TryConfigureLoggingFromFile(RavenConfiguration configuration)
        {
            if (configuration.Logs.ConfigPath == null)
                return false;

#if !RVN
            AsyncHelpers.RunSync(() => InstallAdditionalTargetsAsync(configuration));
#endif
            LogManager.Setup(x => x.LoadConfigurationFromFile(configuration.Logs.ConfigPath.FullPath, optional: false));
            var c = LogManager.Configuration;
#if !RVN
            c.AddRule(AdminLogsRule);
#endif
            DefaultRule = c.FindRuleByName(Constants.Logging.Names.DefaultRuleName);
            if (DefaultRule == null)
                ThrowNoRule(Constants.Logging.Names.DefaultRuleName);

            DefaultAuditRule = c.FindRuleByName(Constants.Logging.Names.DefaultAuditRuleName);
            if (DefaultAuditRule == null)
                ThrowNoRule(Constants.Logging.Names.DefaultAuditRuleName);

            SystemRule = c.FindRuleByName(Constants.Logging.Names.SystemRuleName);
            if (SystemRule == null)
                ThrowNoRule(Constants.Logging.Names.SystemRuleName);

            MicrosoftRule = c.FindRuleByName(Constants.Logging.Names.MicrosoftRuleName);
            if (MicrosoftRule == null)
                ThrowNoRule(Constants.Logging.Names.MicrosoftRuleName);

            LogManager.Setup(x => x.LoadConfiguration(c));
            LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

            return true;
        }

        void ConfigureInternalLogging()
        {
            if (configuration.Logs.NLogInternalPath == null)
                return;

            InternalLogger.LogFile = configuration.Logs.NLogInternalPath.FullPath;
            InternalLogger.LogLevel = configuration.Logs.NLogInternalLevel.ToNLogLogLevel();
            InternalLogger.LogToConsole = configuration.Logs.NLogLogToConsole;
            InternalLogger.LogToConsoleError = configuration.Logs.NLogLogToConsoleError;
        }

        static void ThrowNoRule(string ruleName)
        {
            throw new InvalidOperationException($"Could not find '{ruleName}' rule in the configuration file.");
        }
    }

#if !RVN
    public static void ConfigureAuditLog(this RavenLogManager logManager, RavenServer server, RavenLogger logger)
    {
        var configuration = server.Configuration;

        if (configuration.Security.AuditLogPath == null)
            return;

        if (configuration.Security.AuthenticationEnabled == false)
        {
            if (logger.IsErrorEnabled)
                logger.Error("The audit log configuration 'Security.AuditLog.FolderPath' was specified, but the server is not running in a secured mode. Audit log disabled!");
            return;
        }

        // we have to do this manually because LoggingSource will ignore errors
        AssertCanWriteToAuditLogDirectory(configuration);

        var config = LogManager.Configuration ?? new LoggingConfiguration();

        var fileTarget = new FileTarget
        {
            Name = nameof(FileTarget),
            CreateDirs = true,
            FileName = configuration.Security.AuditLogPath.Combine("${shortdate}.audit.log").FullPath,
            ArchiveNumbering = ArchiveNumberingMode.DateAndSequence,
            Header = Constants.Logging.DefaultHeaderAndFooterLayout,
            Layout = Constants.Logging.DefaultLayout,
            Footer = Constants.Logging.DefaultHeaderAndFooterLayout,
            ConcurrentWrites = false,
            WriteFooterOnArchivingOnly = true,
            ArchiveAboveSize = configuration.Security.AuditLogArchiveAboveSize.GetValue(SizeUnit.Bytes),
            EnableArchiveFileCompression = configuration.Security.AuditLogEnableArchiveFileCompression
        };

        if (configuration.Security.AuditLogMaxArchiveDays.HasValue)
            fileTarget.MaxArchiveDays = configuration.Security.AuditLogMaxArchiveDays.Value;

        if (configuration.Security.AuditLogMaxArchiveFiles.HasValue)
            fileTarget.MaxArchiveFiles = configuration.Security.AuditLogMaxArchiveFiles.Value;

        var fileTargetAsyncWrapper = new AsyncTargetWrapper(nameof(AsyncTargetWrapper), fileTarget);

        DefaultAuditRule.Targets.Clear();
        DefaultAuditRule.Targets.Add(fileTargetAsyncWrapper);

        LogManager.Setup(x => x.LoadConfiguration(config));
        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

        if (RavenLogManager.Instance.IsAuditEnabled)
        {
            var auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();
            auditLog.Audit($"Server started up, listening to {string.Join(", ", configuration.Core.ServerUrls)} with certificate {server.Certificate?.Certificate?.Subject} ({server.Certificate?.Certificate?.Thumbprint}), public url: {configuration.Core.PublicServerUrl}");
        }

        return;

        static void AssertCanWriteToAuditLogDirectory(RavenConfiguration configuration)
        {
            if (Directory.Exists(configuration.Security.AuditLogPath.FullPath) == false)
            {
                try
                {
                    Directory.CreateDirectory(configuration.Security.AuditLogPath.FullPath);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Cannot create audit log directory: {configuration.Security.AuditLogPath.FullPath}, treating this as a fatal error", e);
                }
            }
            try
            {
                var testFile = configuration.Security.AuditLogPath.Combine("write.test").FullPath;
                File.WriteAllText(testFile, "test we can write");
                File.Delete(testFile);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot create new file in audit log directory: {configuration.Security.AuditLogPath.FullPath}, treating this as a fatal error", e);
            }
        }
    }

    public static LogsConfiguration GetLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        var defaultRule = DefaultRule;
        if (defaultRule == null)
            return null;

        var currentMinLevel = defaultRule.Levels.FirstOrDefault() ?? LogLevel.Off;
        var currentMaxLevel = defaultRule.Levels.LastOrDefault() ?? LogLevel.Off;

        var maxArchiveDays = server.Configuration.Logs.MaxArchiveDays;
        var archiveAboveSize = server.Configuration.Logs.ArchiveAboveSize;

        if (TryGetLegacyMaxArchiveDays(server.Configuration, out var legacyMaxArchiveDays))
            maxArchiveDays = legacyMaxArchiveDays;
        if (TryGetLegacyArchiveAboveSize(server.Configuration, out var legacyArchiveAboveSize))
            archiveAboveSize = legacyArchiveAboveSize;

        return new LogsConfiguration
        {
            MinLevel = server.Configuration.Logs.MinLevel,
            MaxLevel = server.Configuration.Logs.MaxLevel,
            MaxArchiveFiles = server.Configuration.Logs.MaxArchiveFiles,
            EnableArchiveFileCompression = server.Configuration.Logs.EnableArchiveFileCompression,
            MaxArchiveDays = maxArchiveDays,
            ArchiveAboveSizeInMb = archiveAboveSize.GetValue(SizeUnit.Megabytes),
            Path = server.Configuration.Logs.Path.FullPath,
            CurrentMinLevel = currentMinLevel.FromNLogLogLevel(),
            CurrentMaxLevel = currentMaxLevel.FromNLogLogLevel(),
            CurrentFilters = GetFilters(defaultRule),
            CurrentLogFilterDefaultAction = defaultRule.FilterDefaultAction.ToLogFilterAction()
        };
    }

    public static AuditLogsConfiguration GetAuditLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        var defaultAuditRule = DefaultAuditRule;
        var currentLevel = defaultAuditRule?.Levels.FirstOrDefault() ?? LogLevel.Off;

        return new AuditLogsConfiguration
        {
            MaxArchiveFiles = server.Configuration.Security.AuditLogMaxArchiveFiles,
            EnableArchiveFileCompression = server.Configuration.Security.AuditLogEnableArchiveFileCompression,
            MaxArchiveDays = server.Configuration.Security.AuditLogMaxArchiveDays,
            ArchiveAboveSizeInMb = server.Configuration.Security.AuditLogArchiveAboveSize.GetValue(SizeUnit.Megabytes),
            Path = server.Configuration.Security.AuditLogPath?.FullPath,
            Level = currentLevel.FromNLogLogLevel(),
        };
    }


    public static MicrosoftLogsConfiguration GetMicrosoftLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        if (DefaultRule == null)
            return null;

        var microsoftRule = MicrosoftRule;
        var currentMinLevel = microsoftRule.FinalMinLevel ?? LogLevel.Off;

        return new MicrosoftLogsConfiguration
        {
            CurrentMinLevel = currentMinLevel.FromNLogFinalMinLogLevel(),
            MinLevel = server.Configuration.Logs.MicrosoftMinLevel
        };
    }

    public static AdminLogsConfiguration GetAdminLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        var webSocketRule = AdminLogsRule;
        var currentMinLevel = webSocketRule.Levels.FirstOrDefault() ?? LogLevel.Off;
        var currentMaxLevel = webSocketRule.Levels.LastOrDefault() ?? LogLevel.Off;

        return new AdminLogsConfiguration
        {
            CurrentMinLevel = currentMinLevel.FromNLogLogLevel(),
            CurrentMaxLevel = currentMaxLevel.FromNLogLogLevel(),
            CurrentFilters = GetFilters(webSocketRule),
            CurrentLogFilterDefaultAction = webSocketRule.FilterDefaultAction.ToLogFilterAction()
        };
    }

    private static List<LogFilter> GetFilters(LoggingRule loggingRule)
    {
        var filters = new List<LogFilter>();

        if (loggingRule.Filters == null || loggingRule.Filters.Count == 0)
            return filters;

        foreach (var filter in loggingRule.Filters)
        {
            if (filter is RavenConditionBasedFilter cFilter)
                filters.Add(cFilter.Filter);
        }

        return filters;
    }

    public static IEnumerable<FileInfo> GetLogFiles(this RavenLogManager logManager, RavenServer server, DateTime? from, DateTime? to)
    {
        AssertNotFileConfig();

        var path = server.Configuration.Logs.Path.FullPath;
        if (Path.Exists(path) == false)
            yield break;

        foreach (var file in Directory.GetFiles(path, "*.log", SearchOption.TopDirectoryOnly))
        {
            var fileInfo = new FileInfo(file);
            var fileName = fileInfo.Name;
            var fileExtension = fileInfo.Extension;
            var fileNameWithoutExtension = fileName;
            if (string.IsNullOrEmpty(fileExtension) == false)
                fileNameWithoutExtension = fileNameWithoutExtension[..^fileExtension.Length];

            var firstIndexOfDot = fileNameWithoutExtension.IndexOf('.');
            if (firstIndexOfDot != -1)
                fileNameWithoutExtension = fileNameWithoutExtension[..firstIndexOfDot];

            if (DateTime.TryParseExact(fileNameWithoutExtension, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateTime) == false)
                continue;

            if (from.HasValue && dateTime < from)
                continue;

            if (to.HasValue && dateTime > to)
                continue;

            yield return fileInfo;
        }
    }

    private static async Task InstallAdditionalTargetsAsync(RavenConfiguration configuration)
    {
        var additionalPackages = configuration.Logs.NuGetAdditionalPackages;
        if (additionalPackages == null || additionalPackages.Count == 0)
            return;

        AppDomain.CurrentDomain.AssemblyResolve += AssemblyResolve;

        foreach (var kvp in additionalPackages)
        {
            try
            {
                var package = await MultiSourceNuGetFetcher.ForLogging.DownloadAsync(kvp.Key, kvp.Value, packageSourceUrl: null);

                RegisterPackage(package);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to register '{kvp.Key} ({kvp.Value})' logging package.", e);
            }
        }

        return;

        static void RegisterPackage(NuGetFetcher.NuGetPackage package)
        {
            if (package == null)
                return;

            foreach (var library in package.Libraries)
            {
                var assembly = LoadAssembly(library);
                if (assembly == null)
                    continue;

                var assemblyName = new AssemblyName(assembly.FullName);
                LoadedAssemblies[assemblyName.Name] = assembly;
            }

            foreach (var dependency in package.Dependencies)
                RegisterPackage(dependency);
        }

        static Assembly LoadAssembly(string path)
        {
            try
            {
                // this allows us to load assembly from runtime if there is a newer one
                // e.g. when we are using newer runtime
                var assemblyName = AssemblyName.GetAssemblyName(path);
                return Assembly.Load(assemblyName);
            }
            catch
            {
                // ignore
            }

            return Assembly.LoadFile(path);
        }
    }

    private static Assembly AssemblyResolve(object sender, ResolveEventArgs args)
    {
        var assemblyName = new AssemblyName(args.Name);
        return LoadedAssemblies.GetValueOrDefault(assemblyName.Name);
    }

    private static void AssertNotFileConfig()
    {
        if (DefaultRule == null)
            throw new InvalidOperationException($"Cannot perform given action, because Logging was configured via NLog.config file.");
    }

    private static bool TryGetLegacyMaxArchiveDays(RavenConfiguration configuration, out int? legacyMaxArchiveDays)
    {
        var retentionTimeInHrs = configuration.GetSetting("Logs.RetentionTimeInHrs");
        if (retentionTimeInHrs != null)
        {
            var setting = new TimeSetting(long.Parse(retentionTimeInHrs), TimeUnit.Hours);
            legacyMaxArchiveDays = (int)setting.GetValue(TimeUnit.Days);
            return true;
        }

        legacyMaxArchiveDays = null;
        return false;
    }

    private static bool TryGetLegacyArchiveAboveSize(RavenConfiguration configuration, out Size legacyArchiveAboveSize)
    {
        var maxFileSizeInMb = configuration.GetSetting("Logs.MaxFileSizeInMb");
        if (maxFileSizeInMb != null)
        {
            legacyArchiveAboveSize = new Size(int.Parse(maxFileSizeInMb), SizeUnit.Megabytes);
            return true;
        }

        legacyArchiveAboveSize = default;
        return false;
    }
#endif
}
