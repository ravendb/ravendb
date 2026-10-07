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
        /// The default. Keeps this search in the group of searches that precede it: the group is emitted as one
        /// parenthesized statement whose members are joined with Or. A search without this flag ends the group.
        /// </summary>
        Guess = 8
    }
}
