using System;
using System.Xml.Linq;
using NuGet.Common;
using NuGet.Packaging;
using NuGet.ProjectManagement;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public class ProjectContext : INuGetProjectContext
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServer>("NuGet");

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

            if (Logger.IsOperationsEnabled)
            {
                switch (level)
                {
                    case MessageLevel.Warning:
                    case MessageLevel.Error:
                        Logger.Operations(string.Format(message, args));
                        break;
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
            if (Logger.IsOperationsEnabled)
                Logger.Operations(message);
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
