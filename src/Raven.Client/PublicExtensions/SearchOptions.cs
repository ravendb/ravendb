using System;

namespace Raven.Client.PublicExtensions
{
    [Flags]
    public enum SearchOptions
    {
        Or = 1,
        And = 2,
        Not = 4,
        Guess = 8
    }
}
