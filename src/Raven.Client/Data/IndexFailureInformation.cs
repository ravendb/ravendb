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
        public bool IsInvalidIndex
        {
            get
            {
                return CheckIndexInvalid(Attempts, Errors);
            }
        }

        public static bool CheckIndexInvalid(int attempts, int errors)
        {
            if (attempts == 0 || errors == 0)
                return false;
            // we don't have enough attempts to make a useful determination
            if (attempts < 100)
                return false;
            return (errors / (float)attempts) > 0.15;
        }

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
