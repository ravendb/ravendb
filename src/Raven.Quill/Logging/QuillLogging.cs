using System.Security.Claims;
using NLog;
using NLog.Common;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Quill.Contracts;
using Raven.Server.Logging;
using Sparrow.Global;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using LogLevel = Sparrow.Logging.LogLevel;
using NLogLevel = NLog.LogLevel;

namespace Raven.Quill.Logging;

public enum QuillLogSource
{
    BuiltIn,

    File,
}

public sealed class QuillLogging
{
    internal const string TemplateFileName = "quill.nlog.template.config";

    internal const string ConfigFileName = "quill.nlog.config";

    internal const string NormalFileName = "quill.log";
    internal const string AuditFileName = "quill.audit.log";
    internal const string NormalTargetName = "QuillLogging";
    internal const string AuditTargetName = "QuillLoggingAudit";
    internal const string AsyncWrapperName = "AsyncTargetWrapper";
    internal const string AuditLoggerName = "Audit";
    private const string AuditResourceName = "Quill";
    private const string WriteProbeFileName = "write.test";

    private const string UnusedTargetNotice = "Unused target detected";

    private readonly LoggingRule _defaultRule;
    private readonly LoggingRule _defaultAuditRule;

    private readonly IReadOnlyList<LoggingRule> _finalMinLevelRules;

    private readonly FileTarget? _fileTarget;
    private readonly FileTarget? _auditTarget;

    private readonly Target? _fileSink;

    private QuillLogging(LogFactory factory, LoggingConfiguration configuration, QuillLogSource source,
        string? loadedFrom, string? configPath, IReadOnlyList<string> configurationProblems)
    {
        Factory = factory;
        Source = source;
        LoadedFrom = loadedFrom;
        ConfigPath = configPath;
        ConfigurationProblems = configurationProblems;

        _defaultRule = FindRule(configuration, Constants.Logging.Names.DefaultRuleName);
        _defaultAuditRule = FindRule(configuration, Constants.Logging.Names.DefaultAuditRuleName);
        _finalMinLevelRules =
        [
            FindRule(configuration, Constants.Logging.Names.MicrosoftRuleName),
            FindRule(configuration, Constants.Logging.Names.SystemRuleName),
        ];

        _fileTarget = configuration.FindTargetByName<FileTarget>(NormalTargetName);
        _auditTarget = configuration.FindTargetByName<FileTarget>(AuditTargetName);
        _fileSink = configuration.FindTargetByName(AsyncWrapperName) ?? _fileTarget;

        MinLevel = CurrentMinLevel;
        MicrosoftMinLevel = CurrentMicrosoftMinLevel;

        AuditLogger = new RavenAuditLogger(factory.GetLogger(AuditLoggerName)
            .WithProperty(Constants.Logging.Properties.Resource, AuditResourceName));
    }

    public LogFactory Factory { get; }

    public QuillLogSource Source { get; }

    public string? LoadedFrom { get; }

    public string? ConfigPath { get; }

    public IReadOnlyList<string> ConfigurationProblems { get; }

    public RavenAuditLogger AuditLogger { get; }

    public LogLevel MinLevel { get; }

    public LogLevel MicrosoftMinLevel { get; }

    public void Audit(string action, string target, HttpContext? context, ClaimsPrincipal? principal = null) =>
        AuditLogger.Audit(action, target, context, principal);

    public bool IsAuditEnabled => _defaultAuditRule.Targets.Any(target => target is not NullTarget);

    public bool IsFileLogEnabled => _fileSink is not null && _defaultRule.Targets.Contains(_fileSink);

    public bool MicrosoftEnabled => CurrentMicrosoftMinLevel != LogLevel.Off;

    public LogLevel CurrentMinLevel => (_defaultRule.Levels.FirstOrDefault() ?? NLogLevel.Off).FromNLogLogLevel();

    public LogLevel CurrentMicrosoftMinLevel =>
        (MicrosoftFinalMinLevel ?? NLogLevel.Off).FromNLogFinalMinLogLevel();

    internal NLogLevel? MicrosoftFinalMinLevel => _finalMinLevelRules[0].FinalMinLevel;

    internal FileTarget? NormalTarget => _fileTarget;

    internal FileTarget? AuditTarget => _auditTarget;

    internal static IEnumerable<string> TargetNames(LoggingRule rule) =>
        rule.Targets.Where(target => target is not NullTarget).Select(target => target.Name);

    internal IEnumerable<string> DefaultTargetNames => TargetNames(_defaultRule);

    internal IEnumerable<string> AuditTargetNames => TargetNames(_defaultAuditRule);

    internal string? CurrentLogDirectory => DirectoryOf(IsFileLogEnabled ? _fileTarget : null);

    internal string? CurrentAuditDirectory => DirectoryOf(IsAuditEnabled ? _auditTarget : null);

    internal string? CurrentLogFile => FileOf(IsFileLogEnabled ? _fileTarget : null);

    internal string? CurrentAuditFile => FileOf(IsAuditEnabled ? _auditTarget : null);

    public static QuillLogging CreateOrFallback(QuillLogOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.ConfigPath is not { } configPath || File.Exists(configPath) == false)
            return CreateBuiltIn(options);

        try
        {
            return Create(configPath, configPath);
        }
        catch (Exception e)
        {
            return CreateBuiltIn(options,
            [
                $"'{configPath}' could not be used, so the appliance is running on its built-in " +
                $"logging defaults: {e.Message}",
            ]);
        }
    }

    public static QuillLogging CreateBuiltIn(QuillLogOptions options, IReadOnlyList<string>? problems = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        return Load(problems, factory => QuillLogDefaults.Build(factory, options),
            QuillLogSource.BuiltIn, loadedFrom: null, options.ConfigPath);
    }

    public static QuillLogging Create(string loadPath, string? configPath = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(loadPath);

        return Load(problems: null, factory => new XmlLoggingConfiguration(loadPath, factory),
            QuillLogSource.File, loadPath, configPath);
    }

    private static QuillLogging Load(IReadOnlyList<string>? problems, Func<LogFactory, LoggingConfiguration> build,
        QuillLogSource source, string? loadedFrom, string? configPath)
    {
        var factory = new LogFactory();
        var collected = problems is null ? new List<string>() : new List<string>(problems);

        void Collect(object? sender, InternalLogEventArgs e)
        {
            if (e.Level < NLogLevel.Warn)
                return;

            var problem = e.Exception?.Message ?? e.Message;

            if (problem.Contains(UnusedTargetNotice, StringComparison.Ordinal))
                return;

            collected.Add(problem);
        }

        var restoreLevel = InternalLogger.LogLevel;
        InternalLogger.InternalEventOccurred += Collect;

        LoggingConfiguration configuration;

        try
        {
            if (restoreLevel > NLogLevel.Warn)
                InternalLogger.LogLevel = NLogLevel.Warn;

            configuration = build(factory);
            factory.Configuration = configuration;
        }
        finally
        {
            InternalLogger.InternalEventOccurred -= Collect;
            InternalLogger.LogLevel = restoreLevel;
        }

        return new QuillLogging(factory, configuration, source, loadedFrom, configPath, collected);
    }

    public LogsConfiguration GetLogsConfiguration()
    {
        var rotation = Rotation(_fileTarget);

        return new LogsConfiguration
        {
            Path = CurrentLogDirectory,
            MinLevel = MinLevel,
            CurrentMinLevel = CurrentMinLevel,
            CurrentFilters = GetFilters(_defaultRule),
            CurrentLogFilterDefaultAction = _defaultRule.FilterDefaultAction.ToLogFilterAction(),
            ArchiveAboveSizeInMb = rotation.SizeInMb,
            MaxArchiveDays = rotation.Days,
            MaxArchiveFiles = rotation.Files,
            EnableArchiveFileCompression = rotation.Compression,
        };
    }

    public AuditLogsConfiguration GetAuditLogsConfiguration()
    {
        var rotation = Rotation(_auditTarget);

        return new AuditLogsConfiguration
        {
            Path = CurrentAuditDirectory,
            Level = IsAuditEnabled ? LogLevel.Info : LogLevel.Off,
            ArchiveAboveSizeInMb = rotation.SizeInMb,
            MaxArchiveDays = rotation.Days,
            MaxArchiveFiles = rotation.Files,
            EnableArchiveFileCompression = rotation.Compression,
        };
    }

    public MicrosoftLogsConfiguration GetMicrosoftLogsConfiguration() => new()
    {
        MinLevel = MicrosoftMinLevel,
        CurrentMinLevel = CurrentMicrosoftMinLevel,
    };

    internal void AssertCanApply(UpdateLogConfigurationRequest update)
    {
        ArgumentNullException.ThrowIfNull(update);

        if (update.IsEmpty)
            throw new InvalidOperationException("logs or microsoftLogs is required.");

        if (update.MicrosoftLogs is not null && MicrosoftEnabled == false)
            throw new InvalidOperationException(
                "Microsoft and System logging is not captured, so its level cannot be changed from here. " +
                $"Give the '{Constants.Logging.Names.MicrosoftRuleName}' and " +
                $"'{Constants.Logging.Names.SystemRuleName}' rules a lower finalMinLevel in " +
                $"'{ConfigPath ?? ConfigFileName}' and restart to switch it on.");

        if (update.Logs is not { } logs || string.IsNullOrWhiteSpace(logs.Path))
            return;

        if (_fileTarget is null)
            throw new InvalidOperationException(
                $"the loaded configuration '{LoadedFrom}' has no '{NormalTargetName}' target, so its path " +
                "cannot be set from here - edit the file instead.");

        var directory = ResolvePath(logs.Path);

        if (TryPrepareDirectory(directory) == false)
            throw new InvalidOperationException(
                $"'logs.path' '{directory}' could not be created or written to.");
    }

    internal void ConfigureLogging(UpdateLogConfigurationRequest update)
    {
        AssertCanApply(update);

        if (update.MicrosoftLogs is { } microsoftLogs)
        {
            var level = microsoftLogs.MinLevel.ToNLogFinalMinLogLevel();

            foreach (var rule in _finalMinLevelRules)
                rule.FinalMinLevel = level;
        }

        if (update.Logs is { } logs)
        {
            _defaultRule.SetLoggingLevels(logs.MinLevel.ToNLogLogLevel(), logs.MinLevel.ToNLogMaxLogLevel());
            ApplyLogPath(logs.Path);
        }

        Factory.ReconfigExistingLoggers(purgeObsoleteLoggers: true);
    }

    public void Flush() => Factory.Flush(TimeSpan.FromSeconds(15));

    private void ApplyLogPath(string? path)
    {
        if (_fileSink is null || _fileTarget is null)
            return;

        if (string.IsNullOrWhiteSpace(path))
        {
            _defaultRule.Targets.Remove(_fileSink);
            return;
        }

        Flush();

        _fileTarget.FileName = new SimpleLayout(Path.Combine(ResolvePath(path), NormalFileName));

        if (_defaultRule.Targets.Contains(_fileSink) == false)
            _defaultRule.Targets.Add(_fileSink);
    }

    private static LoggingRule FindRule(LoggingConfiguration configuration, string ruleName) =>
        configuration.FindRuleByName(ruleName)
        ?? throw new InvalidOperationException(
            $"Could not find the '{ruleName}' rule in the logging configuration. Rules are looked up by " +
            $"name, so they must keep the names the shipped {TemplateFileName} gives them.");

    private static string? FileOf(FileTarget? target) =>
        target?.FileName.Render(LogEventInfo.CreateNullEvent());

    private static string? DirectoryOf(FileTarget? target) =>
        FileOf(target) is { } file ? Path.GetDirectoryName(file) : null;

    private static (long SizeInMb, int? Days, int? Files, bool Compression) Rotation(FileTarget? target) =>
        target is null
            ? (0, null, null, false)
            : (target.ArchiveAboveSize / Constants.Size.Megabyte,
                target.MaxArchiveDays == 0 ? null : target.MaxArchiveDays,
                target.MaxArchiveFiles == 0 ? null : target.MaxArchiveFiles,
                target.EnableArchiveFileCompression);

    private static List<LogFilter> GetFilters(LoggingRule rule)
    {
        var filters = new List<LogFilter>();

        foreach (var filter in rule.Filters)
        {
            if (filter is RavenConditionBasedFilter conditionBasedFilter)
                filters.Add(conditionBasedFilter.Filter);
        }

        return filters;
    }

    internal static string ResolvePath(string path) =>
        Path.IsPathRooted(path) ? path : Path.Combine(AppContext.BaseDirectory, path);

    internal static bool TryPrepareDirectory(string path)
    {
        try
        {
            Directory.CreateDirectory(path);
            var probe = Path.Combine(path, WriteProbeFileName);
            File.WriteAllText(probe, "test we can write");
            File.Delete(probe);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
