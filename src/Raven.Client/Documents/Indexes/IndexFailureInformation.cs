//-----------------------------------------------------------------------
// <copyright file="IndexFailureInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

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
        public bool IsInvalidIndex(Func<bool> isStale)
        {
            return CheckIndexInvalid(MapAttempts, MapErrors, ReduceAttempts, ReduceErrors, isStale);
        }

        public static bool CheckIndexInvalid(long attempts, long errors, long? reduceAttempts, long? reduceErrors, Func<bool> isStale)
        {
            if ((attempts == 0 || errors == 0) && (reduceAttempts == null || reduceAttempts == 0))
                return false;

            if (reduceAttempts != null)
                attempts += reduceAttempts.Value;

            if (reduceErrors != null)
                errors += reduceErrors.Value;

            if (attempts > SufficientNumberOfAttemptsToCheckFailureRate)
                return (errors / (float)attempts) > FailureThreshold;

            // we don't have enough attempts to make a good determination

            if (isStale()) // an index hasn't complete yet, let it index more docs
                return false;

            if (attempts >= MinimalNumberOfAttemptsToCheckFailureRate) // enough to calculate
                return (errors / (float)attempts) > FailureThreshold;
            
            if (attempts == errors) // no results and just errors
                return true;

            return false;
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
        /// Index etag (internal).
        /// </summary>
        public long Etag { get; set; }

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
                return $"Index {Name} ({Etag}) is invalid, out of {MapAttempts} map attempts, {MapErrors} has failed. Error rate of {FailureRate:#.##%} exceeds allowed 15% error rate";

            return $"Index {Name} ({Etag}) is invalid, out of {MapAttempts} map attempts and {ReduceAttempts} reduce attempts, {MapErrors} and {ReduceErrors} has failed respectively. Error rate of {FailureRate:#.##%} exceeds allowed 15% error rate";
        }
    }
}
