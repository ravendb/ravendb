using System;
using System.Xml.Linq;
using NuGet.Common;
using NuGet.Packaging;
using NuGet.ProjectManagement;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public class ProjectContext : INuGetProjectContext
    {
        public PackageExtractionContext PackageExtractionContext { get; set; }

        public ISourceControlManagerProvider SourceControlManagerProvider => null;

        public ExecutionContext ExecutionContext => null;

        public XDocument OriginalPackagesConfig { get; set; }
        public NuGetActionType ActionType { get; set; }
        public Guid OperationId { get; set; }

        public void Log(MessageLevel level, string message, params object[] args)
        {
        }

        public void Log(ILogMessage message)
        {
        }

        public void ReportError(string message)
        {
        }

        public void ReportError(ILogMessage message)
        {
        }

        public FileConflictAction ResolveFileConflict(string message)
        {
            return FileConflictAction.Ignore;
        }
    }
}
