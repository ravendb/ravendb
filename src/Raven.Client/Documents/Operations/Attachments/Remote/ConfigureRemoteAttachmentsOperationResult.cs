namespace Raven.Client.Documents.Operations.Attachments.Remote
{
    /// <summary>
    /// Represents the result of a configure remote attachments operation.
    /// Contains information about the Raft command execution.
    /// </summary>
    public sealed class ConfigureRemoteAttachmentsOperationResult
    {
        /// <summary>
        /// Gets or sets the index of the Raft command that was executed to configure remote attachments.
        /// This value is null if the operation was not executed through the Raft consensus mechanism.
        /// </summary>
        public long? RaftCommandIndex { get; set; }
    }
}
