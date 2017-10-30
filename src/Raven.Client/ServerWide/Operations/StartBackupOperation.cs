using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class StartBackupOperation : IServerOperation<CommandResult>
    {
        private readonly bool _isFullBackup;
        private readonly string _databaseName;
        private readonly long _taskId;

        public StartBackupOperation(bool isFullBackup, string databaseName, long taskId)
        {
            _isFullBackup = isFullBackup;
            _databaseName = databaseName;
            _taskId = taskId;
        }

        public RavenCommand<CommandResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartBackupCommand(_isFullBackup, _databaseName, _taskId);
        }
    }    
}
