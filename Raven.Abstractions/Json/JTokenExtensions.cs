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
		class PathPart
		{
			public string FinalName;
			public string Name;
			public readonly Dictionary<string, PathPart> Items = new Dictionary<string, PathPart>();

			public void ForEach(JToken result, JToken item, Action<PathPart, JToken, JToken> action)
			{
				if (string.IsNullOrEmpty(FinalName) == false)
				{
					action(this, item, result);
					return;
				}
				JToken newResult = GetTheNewResultOrWireTheDefault(result);
				if (item == null)
				{
					foreach (var pathPart in Items)
						pathPart.Value.ForEach(newResult, null, action);
					return;
				}
				if (item is JArray == false)
				{
					foreach (var pathPart in Items)
						pathPart.Value.ForEach(newResult, item.SelectToken(pathPart.Key), action);
				}
				else
				{
					var jArray = newResult as JArray;
					if (jArray == null)
					{
						jArray = new JArray();
						result[Name] = jArray;
					}
					foreach (var subItem in item)
					{
						newResult = new JObject();
						jArray.Add(newResult);
						foreach (var pathPart in Items)
						{
							pathPart.Value.ForEach(newResult, subItem.SelectToken(pathPart.Key), action);
						}
					}
				}
			}

			private JToken GetTheNewResultOrWireTheDefault(JToken result)
			{
				var selectToken = result.SelectToken(Name);
				if (selectToken != null)
					return selectToken;
				return result[Name] = new JObject();
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

		public static JObject SelectTokenWithRavenSyntax(this JToken self, string[] paths)
		{
			var parts = new PathPart();
			foreach (var path in paths)
			{
				var pathParts = path.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
				BuildPathPart(parts, 0, pathParts, path);
			}
			var obj = new JObject();

			foreach (var currentPart in parts.Items)
			{
				currentPart.Value.ForEach(obj, self.SelectToken(currentPart.Key), (part, item, result) =>
				                                                                  	{
				                                                                  		result[part.Name] = item;
				                                                                  	});
			}

			return obj;
		}

		public static IEnumerable<JToken> SelectTokenWithRavenSyntaxReturningFlatStructure(this JToken self, string path)
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
				foreach (var subItem in item.SelectTokenWithRavenSyntaxReturningFlatStructure(string.Join(",", pathParts.Skip(1).ToArray())))
				{
					yield return subItem;
				}
			}
		}
	}
}