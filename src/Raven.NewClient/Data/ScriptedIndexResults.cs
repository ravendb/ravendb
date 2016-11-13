namespace Raven.Abstractions.Data
{
    public class ScriptedIndexResults
    {
        public const string IdPrefix = "Raven/ScriptedIndexResults/";

        /// <summary>
        /// Identifier for ScriptedIndexResults document.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Script that will be evaluated for each indexed document.
        /// </summary>
        public string IndexScript { get; set; }

        /// <summary>
        /// Script that will be avaluated for each document deleted from the index.
        /// </summary>
        public string DeleteScript { get; set; }

        /// <summary>
        /// Indicates if patcher should retry applying the scripts when concurrency exception occurs. If <c>false</c> then exception will be thrown and indexing will fail for this particular batch.
        /// </summary>
        /// <value>By default set to <c>true</c>.</value>
        public bool RetryOnConcurrencyExceptions { get; set; }

        public ScriptedIndexResults()
        {
            RetryOnConcurrencyExceptions = true;
        }

        protected bool Equals(ScriptedIndexResults other)
        {
            return string.Equals(Id, other.Id) && string.Equals(IndexScript, other.IndexScript) && string.Equals(DeleteScript, other.DeleteScript);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ScriptedIndexResults) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Id != null ? Id.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (IndexScript != null ? IndexScript.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (DeleteScript != null ? DeleteScript.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
