// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Indexes;

namespace Raven.Client.Util
{
	public class CSharpClassName
	{
		public static string ConvertToValidClassName(string input)
		{
			var ravenEntityName = input.Replace("-", "_")
			                           .Replace(" ", "_")
			                           .Replace("__", "_");
			if (ExpressionStringBuilder.keywordsInCSharp.Contains(ravenEntityName))
				ravenEntityName += "Item";
			return ravenEntityName;
		}
	}
}