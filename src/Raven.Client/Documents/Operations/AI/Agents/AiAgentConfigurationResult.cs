namespace Raven.Client.Documents.Operations.AI.Agents
{
    /// <summary>
    /// Result returned by the server for an AI agent configuration operation.
    /// </summary>
    public sealed class AiAgentConfigurationResult
    {
        /// <summary>
        /// The AI agent configuration identifier.
        /// </summary>
        public string Identifier { get; set; }

        /// <summary>
        /// Raft index of the command that performed the operation.
        /// </summary>
        public long RaftCommandIndex { get; set; }
    }
}
