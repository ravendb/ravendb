using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;

namespace Raven.Database.Linq.PrivateExtensions
{
	/// <summary>
	/// Extension methods that we are translating on dynamic objects during the 
	/// translation phase of the index compilation
	/// </summary>
	public class DynamicExtensionMethods
	{
		public static BoostedValue Boost(dynamic o, float value)
		{
			return new BoostedValue
			{
				Value = o,
				Boost = value
			};
		}

		public static object IfEntityIs(dynamic o, string entityName)
		{
			if (string.Equals(o[Constants.Metadata][Constants.RavenEntityName], entityName, StringComparison.InvariantCultureIgnoreCase))
				return o;
			return new DynamicNullObject();
		}

		public static object Reverse(object o)
		{
			if (o == null)
				return new DynamicNullObject();

			var s = o as string;
			if (s != null)
				return Reverse(s);

			return Reverse((IEnumerable<object>) o);
		}

		 private static string Reverse(string str)
		 {
		 	var stringBuilder = new StringBuilder(str.Length);
		 	for (int i = str.Length-1; i >= 0; i--)
		 	{
		 		stringBuilder.Append(str[i]);
		 	}
		 	return stringBuilder.ToString();
		 }

		 private static IEnumerable<dynamic> Reverse(IEnumerable<object> self)
		 {
// ReSharper disable InvokeAsExtensionMethod
		 	return Enumerable.Reverse(self);
// ReSharper restore InvokeAsExtensionMethod
		 }
	}
}