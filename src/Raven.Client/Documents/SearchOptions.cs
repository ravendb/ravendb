using System;

namespace Raven.Client.Documents
{
    [Flags]
    public enum SearchOptions
    {
        /// <summary>
        /// Logical Or operator will be used in relation to the previous search statement.
        /// </summary>
        Or = 1,
        /// <summary>
        /// Logical And operator will be used in relation to the previous search statement.
        /// </summary>
        And = 2,
        /// <summary>
        /// The current search statement will be negated.
        /// Can be used in combination with the Or, And, and Guess Flags.
        /// </summary>
        Not = 4,
        /// <summary>
        /// RavenDB will attempt to match up semantics between search statements in the following manner: 
        /// An AND operator will be used between a non-search statement and the immediate search statement that follows.
        /// Consecutive searches will be ORed together.
        /// </summary>
        Guess = 8
    }
}
