namespace Raven.Client.Documents.Operations.AI.Agents
{
    /// <summary>
    /// The result returned after creating, updating, or deleting an AI agent configuration.
    /// </summary>
    public sealed class AiAgentConfigurationResult
    {
        /// <summary>
        /// The identifier of the affected AI agent configuration.
        /// </summary>
        public string Identifier { get; set; }
        /// <summary>
        /// The Raft index of the command that performed the operation.
        /// </summary>
        public long RaftCommandIndex { get; set; }
    }
}
