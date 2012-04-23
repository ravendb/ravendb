using System;

namespace Raven.Client.Linq
{
	[Flags]
	public enum SearchOptions
	{
		Or = 0,
		And = 1,
		Not = 4
	}

	public enum EscapeQueryOptions
	{
		EscapeAll,
		AllowPostfixWildcard,
		AllowAllWildcards,
		RawQuery
	}
}