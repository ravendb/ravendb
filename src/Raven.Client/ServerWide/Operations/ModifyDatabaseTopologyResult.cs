namespace Raven.Client.ServerWide.Operations
{
    public sealed class ModifyDatabaseTopologyResult
    {
        /// <summary>
        /// The Raft Command Index that was executed 
        /// </summary>
        public long RaftCommandIndex { get; set; }
    }
}