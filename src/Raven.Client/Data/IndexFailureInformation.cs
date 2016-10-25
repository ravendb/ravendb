//-----------------------------------------------------------------------
// <copyright file="IndexFailureInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Client.Data
{
    /// <summary>
    /// Information about index failure rates
    /// </summary>
    public class IndexFailureInformation
    {
        private const float FailureThreshold = 0.15f;

        /// <summary>
        /// Indicates whether this is invalid index.
        /// </summary>
        /// <value><c>true</c> if this is invalid index; otherwise, <c>false</c>.</value>
        public bool IsInvalidIndex => CheckIndexInvalid(MapAttempts, MapErrors, ReduceAttempts, ReduceErrors);

        public static bool CheckIndexInvalid(long attempts, long errors, long? reduceAttempts, long? reduceErrors)
        {
            if ((attempts == 0 || errors == 0) && (reduceAttempts == null || reduceAttempts == 0))
                return false;
            if (reduceAttempts != null)
            {
                // we don't have enough attempts to make a useful determination
                if (attempts + reduceAttempts < 100)
                    return false;
                return (errors + (reduceErrors ?? 0)) / (float)(attempts + (reduceAttempts ?? 0)) > FailureThreshold;
            }
            // we don't have enough attempts to make a useful determination
            if (attempts < 100)
                return false;
            return (errors / (float)attempts) > FailureThreshold;
        }

        /// <summary>
        /// Number of reduce attempts.
        /// </summary>
        public long? ReduceAttempts { get; set; }

        /// <summary>
        /// Number of reduce errors.
        /// </summary>
        public long? ReduceErrors { get; set; }

        /// <summary>
        /// Number of reduce successes.
        /// </summary>
        public long? ReduceSuccesses { get; set; }

        /// <summary>
        /// Index id (internal).
        /// </summary>
        public int IndexId { get; set; }

        /// <summary>
        /// Index name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Number of indexing attempts.
        /// </summary>
        public long MapAttempts { get; set; }

        /// <summary>
        /// Number of indexing errors.
        /// </summary>
        public long MapErrors { get; set; }

        /// <summary>
        /// Number of indexing successes.
        /// </summary>
        public long MapSuccesses { get; set; }

        /// <summary>
        /// Failure rate.
        /// </summary>
        public float FailureRate
        {
            get
            {
                var attempts = MapAttempts;
                if (ReduceAttempts.HasValue)
                    attempts += ReduceAttempts.Value;

                if (attempts == 0)
                    return 0;

                var errors = MapErrors;
                if (ReduceErrors.HasValue)
                    errors += ReduceErrors.Value;

                return errors / (float)errors;
            }
        }

        /// <summary>
        /// Error message.
        /// </summary>
        /// <returns></returns>
        public string GetErrorMessage()
        {
            if (ReduceAttempts.HasValue == false)
                return $"Index {Name} ({IndexId}) is invalid, out of {MapAttempts} map attempts, {MapErrors} has failed. Error rate of {FailureRate:#.##%} exceeds allowed 15% error rate";

            return $"Index {Name} ({IndexId}) is invalid, out of {MapAttempts} map attempts and {ReduceAttempts} reduce attempts, {MapErrors} and {ReduceErrors} has failed respectively. Error rate of {FailureRate:#.##%} exceeds allowed 15% error rate";
        }
    }
}
