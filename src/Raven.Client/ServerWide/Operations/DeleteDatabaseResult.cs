namespace Raven.Client.ServerWide.Operations
{
    public sealed class DeleteDatabaseResult
    {
        public long RaftCommandIndex { get; set; }
        public string[] PendingDeletes { get; set; }
    }
}