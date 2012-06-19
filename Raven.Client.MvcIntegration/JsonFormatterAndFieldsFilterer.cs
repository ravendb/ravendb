using System;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Client.Connection.Profiling;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Client.MvcIntegration
{
	public class JsonFormatterAndFieldsFilterer
	{
		private readonly HashSet<string> fieldsToFilter = new HashSet<string>();

		public JsonFormatterAndFieldsFilterer(HashSet<string> fieldsToFilter)
		{
			this.fieldsToFilter = fieldsToFilter;
		}

		public ProfilingInformation Filter(ProfilingInformation information)
		{
			var profilingInformation = ProfilingInformation.CreateProfilingInformation(information.Id);
			profilingInformation.At = information.At;
			profilingInformation.Context = information.Context;
			profilingInformation.DurationMilliseconds = information.DurationMilliseconds;
			profilingInformation.Requests = information.Requests.Select(FilterRequest).ToList();
			return profilingInformation;
		}

		private RequestResultArgs FilterRequest(RequestResultArgs input)
		{
			return new RequestResultArgs
			{
				DurationMilliseconds = input.DurationMilliseconds,
				At = input.At,
				HttpResult = input.HttpResult,
				Method = input.Method,
				Status = input.Status,
				Url = input.Url,
				PostedData = FilterData(input.PostedData),
				Result = FilterData(input.Result)
				
			};
		}

		private string FilterData(string result)
		{
			RavenJToken token;
			try
			{
				token = RavenJToken.Parse(result);
			}
			catch (Exception)
			{
				return result;
			}
			Visit(token);
			return token.ToString(Formatting.Indented);
		}

		private void Visit(RavenJToken token)
		{
			switch (token.Type)
			{
				case JTokenType.Object:
					var obj = (RavenJObject) token;
					Action after = () => { };
					foreach (var item in obj)
					{
						Visit(item.Value);

						if (fieldsToFilter.Contains(item.Key))
						{
							var itemCopy = item;
							after += () => obj[itemCopy.Key] = "...private...";
						}
					}

					after();
					break;
				case JTokenType.Array:
					foreach (var items in ((RavenJArray)token))
					{
						Visit(items);
					}
					break;
				case JTokenType.Constructor:
				case JTokenType.Property:
				case JTokenType.Comment:
				case JTokenType.None:
				case JTokenType.Integer:
				case JTokenType.Float:
				case JTokenType.String:
				case JTokenType.Boolean:
				case JTokenType.Null:
				case JTokenType.Undefined:
				case JTokenType.Date:
				case JTokenType.Raw:
				case JTokenType.Bytes:
					break;
				default:
					throw new ArgumentOutOfRangeException(token.Type.ToString());
			}
		}
	}
}