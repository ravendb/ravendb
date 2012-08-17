using System;
using System.Collections.Generic;
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Json
{
	public class ConflictsResolver
	{
		private readonly RavenJObject[] docs;
		readonly RavenJTokenEqualityComparer ravenJTokenEqualityComparer = new RavenJTokenEqualityComparer();

		public ConflictsResolver(params RavenJObject[] docs)
		{
			this.docs = docs;
		}

		public string Resolve(int indent = 1)
		{
			var result = new Dictionary<string, object>();
			for (int index = 0; index < docs.Length; index++)
			{
				var doc = docs[index];
				foreach (var prop in doc)
				{
					if (result.ContainsKey(prop.Key)) // already dealt with
						continue;

					switch (prop.Value.Type)
					{
						case JTokenType.Object:
							if(TryHandleObjectValue(index, result, prop) == false)
								goto default;
							break;
						case JTokenType.Array:
							if(TryHandleArrayValue(index, result, prop) == false)
								goto default;
							break;
						default:
							HandleSimpleValues(result, prop, index);
							break;
					}
				}
			}
			return GenerateOutput(result, indent);
		}

		private bool TryHandleArrayValue(int index, Dictionary<string, object> result, KeyValuePair<string, RavenJToken> prop)
		{
			var arrays = new List<RavenJArray>
			{
				(RavenJArray)prop.Value
			};
			for (int i = 0; i < docs.Length; i++)
			{
				if (i == index)
					continue;

				RavenJToken token;
				if (docs[i].TryGetValue(prop.Key, out token) && token.Type != JTokenType.Array)
					return false;
				if (token == null)
					continue;
				arrays.Add((RavenJArray)token);
			}

			var mergedArray = new RavenJArray();
			while (arrays.Count > 0)
			{
				var set = new HashSet<RavenJToken>(ravenJTokenEqualityComparer);
				for (int i = 0; i < arrays.Count; i++)
				{
					set.Add(arrays[i][0]);
					arrays[i].RemoveAt(0);
					if(arrays[i].Length == 0)
						arrays.RemoveAt(i);
				}
				foreach (var ravenJToken in set)
				{
					mergedArray.Add(ravenJToken);
				}
			}

			if (ravenJTokenEqualityComparer.Equals(mergedArray, prop.Value))
			{
				result.Add(prop.Key, mergedArray); 
				return true;
			}

			result.Add(prop.Key, new ArrayWithWarning(mergedArray));
			return true;
		}

		private bool TryHandleObjectValue(int index, Dictionary<string, object> result, KeyValuePair<string, RavenJToken> prop)
		{
			var others = new List<RavenJObject>
			{
				(RavenJObject)prop.Value
			};
			for (int i = 0; i < docs.Length; i++)
			{
				if(i == index)
					continue;

				RavenJToken token;
				if (docs[i].TryGetValue(prop.Key, out token) && token.Type != JTokenType.Object)
					return false;
				if(token == null)
					continue;
				others.Add((RavenJObject)token);
			}
			result.Add(prop.Key, new ConflictsResolver(others.ToArray()));
			return true;
		}

		private void HandleSimpleValues(Dictionary<string, object> result,
			KeyValuePair<string, RavenJToken> prop, 
			int index)
		{
			var conflicted = new Conflicted
			{
				Values = { prop.Value }
			};
			for (int i = 0; i < docs.Length; i++)
			{
				if(i == index)
					continue;
				var other = docs[i];
			
				RavenJToken otherVal;
				if (other.TryGetValue(prop.Key, out otherVal) == false)
					continue;
				if (ravenJTokenEqualityComparer.Equals(prop.Value, otherVal) == false)
					conflicted.Values.Add(otherVal);
			}
			if (conflicted.Values.Count == 1)
				result.Add(prop.Key, prop.Value);
			else
				result.Add(prop.Key, conflicted);
		}

		private static string GenerateOutput(Dictionary<string, object> result, int indent)
		{
			var stringWriter = new StringWriter();
			var writer = new JsonTextWriter(stringWriter)
			{
				Formatting = Formatting.Indented
			};
			writer.WriteStartObject();
			foreach (var o in result)
			{
				writer.WritePropertyName(o.Key);
				var ravenJToken = o.Value as RavenJToken;
				if (ravenJToken != null)
				{
					ravenJToken.WriteTo(writer);
					continue;
				}
				var conflicted = o.Value as Conflicted;
				if (conflicted != null)
				{
					writer.WriteComment(">>>> conflict start");
					writer.WriteStartArray();
					foreach (var token in conflicted.Values)
					{
						token.WriteTo(writer);
					}
					writer.WriteEndArray();
					writer.WriteComment("<<<< conflict end");
					continue;
				}
				var arrayWithWarning = o.Value as ArrayWithWarning;
				if(arrayWithWarning != null)
				{
					writer.WriteComment(">>>> auto merged array start");
					arrayWithWarning.MergedArray.WriteTo(writer);
					writer.WriteComment("<<<< auto merged array end");
					continue;
				}
				var resolver = o.Value as ConflictsResolver;
				if(resolver != null)
				{
					using(var stringReader = new StringReader(resolver.Resolve(indent + 1)))
					{
						bool first = true;
						string line ;
						while((line = stringReader.ReadLine()) != null)
						{
							if(first == false)
							{
								writer.WriteRaw(Environment.NewLine);
								for (var i = 0; i < indent; i++)
								{
									writer.WriteRaw(new string(writer.IndentChar, writer.Indentation));
								}
							}
							first = false;
							writer.WriteRaw(line);
						}
					}
					continue;
				}
				throw new InvalidOperationException("Could not understand how to deal with: " + o.Value);
			}
			writer.WriteEndObject();
			return stringWriter.GetStringBuilder().ToString();
		}

		private class Conflicted
		{
			public readonly HashSet<RavenJToken> Values = new HashSet<RavenJToken>(new RavenJTokenEqualityComparer());
		}

		private class ArrayWithWarning
		{
			public readonly RavenJArray MergedArray;

			public ArrayWithWarning(RavenJArray mergedArray)
			{
				this.MergedArray = mergedArray;
			}
		}

	}

}