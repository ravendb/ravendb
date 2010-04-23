using System.Collections;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using System.Linq;
using System;

namespace Raven.StackOverflow.Etl.Generic
{
	public class RowToDatabase : AbstractOperation
	{
		private readonly string collection;
		private readonly Func<JObject, string> generateKey;

		public RowToDatabase(
			string collection,
			Func<JObject, string> generateKey)
		{
			this.collection = collection;
			this.generateKey = generateKey;
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			int count = 0;
			foreach (var partitionedRows in rows.Partition(Constants.BatchSize))
			{
				var jsons = partitionedRows.Select(row =>
					  new JObject(row.Cast<DictionaryEntry>()
							.Select(x => new JProperty(x.Key.ToString(), x.Value is JToken ? x.Value : new JValue(x.Value)))
						)
					);
				var putCommandDatas = jsons.Select(document => new PutCommandData
				{
					Document = document,
					Metadata = new JObject(new JProperty("Raven-Entity-Name", new JValue(collection))),
					Key = generateKey(document)
				}).ToArray();

				count++;
				File.WriteAllText(Path.Combine("Docs", collection + " #" + count + ".json"),
								  new JArray(putCommandDatas.Select(x => x.ToJson())).ToString(Formatting.Indented));

			}
			yield break;
		}
	}
}