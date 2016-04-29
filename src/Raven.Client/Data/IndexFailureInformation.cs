//-----------------------------------------------------------------------
// <copyright file="IndexFailureInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
    /// <summary>
    /// Information about index failure rates
    /// </summary>
    public class IndexFailureInformation
    {
        /// <summary>
        /// Indicates whether this is invalid index.
        /// </summary>
        /// <value><c>true</c> if this is invalid index; otherwise, <c>false</c>.</value>
        public bool IsInvalidIndex => CheckIndexInvalid(Attempts, Errors, ReduceAttempts, ReduceErrors);

        public static bool CheckIndexInvalid(int attempts, int errors, int? reduceAttempts, int? reduceErrors)
        {
            if ((attempts == 0 || errors == 0) && (reduceAttempts == null || reduceAttempts == 0))
                return false;
            if (reduceAttempts != null)
            {
                // we don't have enough attempts to make a useful determination
                if (attempts + reduceAttempts < 100)
                    return false;
                return (errors + (reduceErrors ?? 0)) / (float)(attempts + (reduceAttempts ?? 0)) > 0.15;
            }
            // we don't have enough attempts to make a useful determination
            if (attempts < 100)
                return false;
            return (errors / (float)attempts) > 0.15;
        }

        /// <summary>
        /// Number of reduce attempts.
        /// </summary>
        public int? ReduceAttempts { get; set; }

        /// <summary>
        /// Number of reduce errors.
        /// </summary>
        public int? ReduceErrors { get; set; }

        /// <summary>
        /// Number of reduce successes.
        /// </summary>
        public int? ReduceSuccesses { get; set; }

        /// <summary>
        /// Index id (internal).
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Number of indexing attempts.
        /// </summary>
        public int Attempts { get; set; }

        /// <summary>
        /// Number of indexing errors.
        /// </summary>
        public int Errors { get; set; }

        /// <summary>
        /// Number of indexing successes.
        /// </summary>
        public int Successes { get; set; }

        /// <summary>
        /// Failure rate.
        /// </summary>
        public float FailureRate
        {
            get
            {
                if (Attempts == 0)
                    return 0;
                return (Errors / (float)Attempts);
            }
        }

        /// <summary>
        /// Error message.
        /// </summary>
        /// <returns></returns>
        public string GetErrorMessage()
        {
            const string msg =
                "Index {0} is invalid, out of {1} indexing attempts, {2} has failed.\r\nError rate of {3:#.##%} exceeds allowed 15% error rate";
            return string.Format(msg,
                                 Id, Attempts, Errors, FailureRate);
        }
    }
}
