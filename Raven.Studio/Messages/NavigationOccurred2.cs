using System;
using System.Collections.Generic;

namespace Raven.Studio.Messages
{
	public class NavigationOccurred2
	{
		public Type Type { get; private set; }

		public NavigationOccurred2(Type type, Dictionary<string, string> parameters)
		{
			Type = type;
		}
	}
}