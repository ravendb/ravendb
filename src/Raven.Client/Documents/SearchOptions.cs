using System;

namespace Raven.Client.Documents
{
    [Flags]
    public enum SearchOptions
    {
        /// <summary>
        /// Logical Or operator will be used in relation to previous statement
        /// </summary>
        Or = 1,
        /// <summary>
        /// Logical And operator will be used in relation to previous statement
        /// </summary>
        And = 2,
        /// <summary>
        /// Current statement will be negated, can be used in combination with the Or, And and Guess Flags
        /// </summary>
        Not = 4,
        /// <summary>
        /// RavenDB will attemt to matchup semantics between terms in the next manner: 
        /// If there are consecutive searches, they will be OR together, otherwise the AND semantic will be used
        /// </summary>
        Guess = 8
    }
}
