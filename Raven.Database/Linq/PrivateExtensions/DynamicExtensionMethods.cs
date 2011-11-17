using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Linq.PrivateExtensions
{
	/// <summary>
	/// Extension methods that we are translating on dynamic objects during the 
	/// translation phase of the indx compilation
	/// </summary>
	public class DynamicExtensionMethods
	{
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