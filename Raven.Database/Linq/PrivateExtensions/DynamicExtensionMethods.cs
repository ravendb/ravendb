using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;

namespace Raven.Database.Linq.PrivateExtensions
{
	/// <summary>
	/// Extension methods that we are translating on dynamic objects during the 
	/// translation phase of the indx compilation
	/// </summary>
	public class DynamicExtensionMethods
	{
		public static object IfEntityIs(dynamic o, string entityName)
		{
			if (string.Equals(o[Constants.Metadata][Constants.RavenEntityName], entityName, StringComparison.InvariantCultureIgnoreCase))
				return o;
			return new DynamicNullObject();
		}

		 public static string Reverse(string str)
		 {
		 	var stringBuilder = new StringBuilder(str.Length);
		 	for (int i = str.Length-1; i >= 0; i--)
		 	{
		 		stringBuilder.Append(str[i]);
		 	}
		 	return stringBuilder.ToString();
		 }

		 public static IEnumerable<dynamic> Reverse(IEnumerable<object> self)
		 {
// ReSharper disable InvokeAsExtensionMethod
		 	return Enumerable.Reverse(self);
// ReSharper restore InvokeAsExtensionMethod
		 }
	}
}