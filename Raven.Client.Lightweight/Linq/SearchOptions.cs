using System;

namespace Raven.Client.Linq
{
	[Flags]
	public enum SearchOptions
	{
		Or = 0,
		And = 1,
	}
}