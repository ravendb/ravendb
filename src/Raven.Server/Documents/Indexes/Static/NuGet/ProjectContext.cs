using System;
using System.Xml.Linq;
using NuGet.Common;
using NuGet.Packaging;
using NuGet.ProjectManagement;
using Raven.Server.Logging;
using Sparrow.Logging;
using LogLevel = NuGet.Common.LogLevel;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public sealed class ProjectContext : INuGetProjectContext
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<ProjectContext>();

        public PackageExtractionContext PackageExtractionContext { get; set; }

        public ISourceControlManagerProvider SourceControlManagerProvider => null;

        public ExecutionContext ExecutionContext => null;

        public XDocument OriginalPackagesConfig { get; set; }
        public NuGetActionType ActionType { get; set; }
        public Guid OperationId { get; set; }

        public void Log(MessageLevel level, string message, params object[] args)
        {
            if (level == MessageLevel.Debug)
                return;

            if (Logger.IsInfoEnabled)
            {
                Logger.Info(string.Format(message, args));
                return;
            }

            var logLevel = GetLogLevel(level);

            if (Logger.IsEnabled(logLevel))
            {
                Logger.Log(logLevel, string.Format(message, args));
            }

            static Sparrow.Logging.LogLevel GetLogLevel(MessageLevel level)
            {
                switch (level)
                {
                    case MessageLevel.Info:
                        return Sparrow.Logging.LogLevel.Info;
                    case MessageLevel.Warning:
                        return Sparrow.Logging.LogLevel.Warn;
                    case MessageLevel.Debug:
                        return Sparrow.Logging.LogLevel.Debug;
                    case MessageLevel.Error:
                        return Sparrow.Logging.LogLevel.Error;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(level), level, null);
                }
            }
        }

        public void Log(ILogMessage message)
        {
            switch (message.Level)
            {
                case LogLevel.Debug:
                case LogLevel.Verbose:
                    Log(MessageLevel.Debug, message.Message);
                    return;

                case LogLevel.Information:
                case LogLevel.Minimal:
                    Log(MessageLevel.Info, message.Message);
                    return;

                case LogLevel.Warning:
                    Log(MessageLevel.Warning, message.Message);
                    return;

                case LogLevel.Error:
                    Log(MessageLevel.Error, message.Message);
                    return;
            }
        }

        public void ReportError(string message)
        {
            if (Logger.IsErrorEnabled)
                Logger.Error(message);
        }

        public void ReportError(ILogMessage message)
        {
            switch (message.Level)
            {
                case LogLevel.Error:
                    ReportError(message.Message);
                    return;
            }
        }

        public FileConflictAction ResolveFileConflict(string message)
        {
            return FileConflictAction.Ignore;
        }
    }
}
