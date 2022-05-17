//-----------------------------------------------------------------------
// <copyright file="IndexFailureInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Information about index failure rates
    /// </summary>
    public class IndexFailureInformation
    {
        private const float FailureThreshold = 0.15f;

        internal const int SufficientNumberOfAttemptsToCheckFailureRate = 100;

        internal const int MinimalNumberOfAttemptsToCheckFailureRate = (int)(SufficientNumberOfAttemptsToCheckFailureRate / (FailureThreshold * 100));

        /// <summary>
        /// Indicates whether this is invalid index.
        /// </summary>
        /// <value><c>true</c> if this is invalid index; otherwise, <c>false</c>.</value>
        public bool IsInvalidIndex(bool isStale)
        {
            return CheckIndexInvalid(MapAttempts, MapErrors,
                MapReferenceAttempts, MapReferenceErrors,
                ReduceAttempts, ReduceErrors, isStale);
        }

        public static bool CheckIndexInvalid(
            long mapAttempts, long mapErrors,
            long? mapReferenceAttempts, long? mapReferenceErrors,
            long? reduceAttempts, long? reduceErrors,
            bool isStale)
        {
            var attempts = mapAttempts;
            if (mapReferenceAttempts != null)
                attempts += mapReferenceAttempts.Value;
            if (reduceAttempts != null)
                attempts += reduceAttempts.Value;

            var errors = mapErrors;
            if (mapReferenceErrors != null)
                errors += mapReferenceErrors.Value;
            if (reduceErrors != null)
                errors += reduceErrors.Value;

            if (attempts == 0 || errors == 0)
                return false;

            if (attempts > SufficientNumberOfAttemptsToCheckFailureRate)
                return (errors / (float)attempts) > FailureThreshold;

            // we don't have enough attempts to make a good determination

            if (isStale) // an index hasn't complete yet, let it index more docs
                return false;

            if (attempts >= MinimalNumberOfAttemptsToCheckFailureRate) // enough to calculate
                return (errors / (float)attempts) > FailureThreshold;
            
            if (attempts == errors) // no results and just errors
                return true;

            return false;
        }

        /// <summary>
        /// Index name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Number of indexing attempts.
        /// </summary>
        public long MapAttempts { get; set; }

        /// <summary>
        /// Number of indexing successes.
        /// </summary>
        public long MapSuccesses { get; set; }

        /// <summary>
        /// Number of indexing errors.
        /// </summary>
        public long MapErrors { get; set; }

        /// <summary>
        /// Number of reference indexing attempts.
        /// </summary>
        public long? MapReferenceAttempts { get; set; }

        /// <summary>
        /// Number of references indexing successes.
        /// </summary>
        public long? MapReferenceSuccesses { get; set; }

        /// <summary>
        /// Number of reference indexing errors.
        /// </summary>
        public long? MapReferenceErrors { get; set; }

        /// <summary>
        /// Number of reduce attempts.
        /// </summary>
        public long? ReduceAttempts { get; set; }

        /// <summary>
        /// Number of reduce successes.
        /// </summary>
        public long? ReduceSuccesses { get; set; }

        /// <summary>
        /// Number of reduce errors.
        /// </summary>
        public long? ReduceErrors { get; set; }

        /// <summary>
        /// Failure rate.
        /// </summary>
        public float FailureRate
        {
            get
            {
                var attempts = MapAttempts;
                if (MapReferenceAttempts.HasValue)
                    attempts += MapReferenceAttempts.Value;
                if (ReduceAttempts.HasValue)
                    attempts += ReduceAttempts.Value;

                if (attempts == 0)
                    return 0;

                var errors = MapErrors;
                if (MapReferenceErrors.HasValue)
                    errors += MapReferenceErrors.Value;
                if (ReduceErrors.HasValue)
                    errors += ReduceErrors.Value;

                return errors / (float)attempts;
            }
        }

        /// <summary>
        /// Error message.
        /// </summary>
        /// <returns></returns>
        public string GetErrorMessage()
        {
            if (ReduceAttempts.HasValue == false)
                return $"Index {Name} is invalid, out of {MapAttempts} map attempts, {MapErrors} has failed. Error rate of {FailureRate:#.##%} exceeds allowed 15% error rate";

            return $"Index {Name} is invalid, out of {MapAttempts} map attempts and {ReduceAttempts} reduce attempts, {MapErrors} and {ReduceErrors} has failed respectively. Error rate of {FailureRate:#.##%} exceeds allowed 15% error rate";
        }
    }
}
