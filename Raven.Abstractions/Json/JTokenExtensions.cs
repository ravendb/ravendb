//-----------------------------------------------------------------------
// <copyright file="JTokenExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Json
{
	/// <summary>
	/// Extensions for JToken
	/// </summary>
	public static class JTokenExtensions
	{
		class PathPart
		{
			public string FinalName;
			public string Name;
			public readonly Dictionary<string, PathPart> Items = new Dictionary<string, PathPart>();

			public void ForEach(RavenJToken result, RavenJToken item, Action<PathPart, RavenJToken, RavenJToken> action)
			{
				if (string.IsNullOrEmpty(FinalName) == false)
				{
					action(this, item, result);
					return;
				}
				RavenJToken newResult = GetTheNewResultOrWireTheDefault(result);
				if (item == null)
				{
					foreach (var pathPart in Items)
						pathPart.Value.ForEach(newResult, null, action);
					return;
				}
				if (item is RavenJArray == false)
				{
					foreach (var pathPart in Items)
						pathPart.Value.ForEach(newResult, item.SelectToken(pathPart.Key), action);
				}
				else
				{
					var jArray = newResult as RavenJArray;
					if (jArray == null)
					{
						jArray = new RavenJArray();
						result[Name] = jArray;
					}
					foreach (var subItem in item.Children())
					{
						newResult = new RavenJObject();
						jArray.Items.Add(newResult);
						foreach (var pathPart in Items)
						{
							pathPart.Value.ForEach(newResult, subItem.SelectToken(pathPart.Key), action);
						}
					}
				}
			}

			private RavenJToken GetTheNewResultOrWireTheDefault(RavenJToken result)
			{
				var selectToken = result.SelectToken(Name);
				if (selectToken != null)
					return selectToken;
				return result[Name] = new RavenJObject();
			}
		}

		private static void BuildPathPart(PathPart pathPart, int pos, string[] pathParts, string final)
		{
			if (pathParts.Length == pos)
			{
				pathPart.FinalName = final;
				return;
			}
			PathPart part;
			if (pathPart.Items.TryGetValue(pathParts[pos], out part) == false)
			{
				pathPart.Items[pathParts[pos]] = part = new PathPart
				                                        	{
				                                        		Name = pathParts[pos]
				                                        	};
			}
			BuildPathPart(part, pos + 1, pathParts, final);
		}

		public static RavenJObject SelectTokenWithRavenSyntax(this RavenJToken self, string[] paths)
		{
			var parts = new PathPart();
			foreach (var path in paths)
			{
				var pathParts = path.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
				BuildPathPart(parts, 0, pathParts, path);
			}
			var obj = new RavenJObject();

			foreach (var currentPart in parts.Items)
			{
				currentPart.Value.ForEach(obj, self.SelectToken(currentPart.Key), (part, item, result) =>
				                                                                  	{
				                                                                  		result[part.Name] = item;
				                                                                  	});
			}

			return obj;
		}

		public static IEnumerable<RavenJToken> SelectTokenWithRavenSyntaxReturningFlatStructure(this RavenJToken self, string path)
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
			foreach (var item in result.Children())
			{
				foreach (var subItem in item.SelectTokenWithRavenSyntaxReturningFlatStructure(string.Join(",", pathParts.Skip(1).ToArray())))
				{
					yield return subItem;
				}
			}
		}
	}
}