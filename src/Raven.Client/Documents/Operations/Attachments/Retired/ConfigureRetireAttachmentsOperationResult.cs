namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    /// <summary>
    /// Represents the result of a configure retired attachments operation.
    /// Contains information about the Raft command execution.
    /// </summary>
    public sealed class ConfigureRetireAttachmentsOperationResult
    {
        /// <summary>
        /// Gets or sets the index of the Raft command that was executed to configure retired attachments.
        /// This value is null if the operation was not executed through the Raft consensus mechanism.
        /// </summary>
        public long? RaftCommandIndex { get; set; }
    }
}
