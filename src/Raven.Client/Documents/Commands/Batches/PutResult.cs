namespace Raven.Client.Documents.Commands.Batches
{
    /// <summary>
    /// The result of a PUT operation
    /// </summary>
    public sealed class PutResult
    {
        /// <summary>
        /// Id of the document that was PUT.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Change Vector of the document after PUT operation.
        /// </summary>
        public string ChangeVector { get; set; }
    }
}