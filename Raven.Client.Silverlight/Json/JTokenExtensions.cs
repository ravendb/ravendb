//-----------------------------------------------------------------------
// <copyright file="JTokenExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Abstractions.Json
{
	/// <summary>
	/// Extensions for JToken
	/// </summary>
	public static class JTokenExtensions
	{
		public static IEnumerable<JToken> SelectTokenWithRavenSyntax(this JToken self, string path)
		{
			var pathParts = path.Split(new[]{','}, StringSplitOptions.RemoveEmptyEntries);
			var result = self.SelectToken(pathParts[0]);
			if(pathParts.Length == 1)
			{
				yield return result;
				yield break;
			}
			if(result == null)
			{
				yield break;
			}
			foreach (var item in result)
			{
				foreach (var subItem in item.SelectTokenWithRavenSyntax(string.Join(",", pathParts.Skip(1).ToArray())))
				{
					yield return subItem;
				}
			}
		}
	}
}