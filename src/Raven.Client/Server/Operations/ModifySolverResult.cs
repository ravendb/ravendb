namespace Raven.Client.Server.Operations
{
    public class ModifySolverResult
    {
        /// <summary>
        /// Key of the database .
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// long? of the database after PUT operation.
        /// </summary>
        public long? ETag { get; set; }

        public ConflictSolver Solver { get; set; }
    }
}