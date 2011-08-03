using System;
using Raven.Json.Linq;

namespace Raven.Client.Util
{
	public static class Types
	{
		public static bool IsEntityType(this Type type)
		{
			return type != typeof (object) && type != typeof (RavenJObject);
		}
	}
}