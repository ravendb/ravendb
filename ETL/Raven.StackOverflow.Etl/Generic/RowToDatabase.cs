//-----------------------------------------------------------------------
// <copyright file="RowToDatabase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Commands;
using Raven.Database.Data;
using Raven.Json.Linq;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using System.Linq;
using System;


namespace Raven.StackOverflow.Etl.Generic
{
	public class RowToDatabase : AbstractOperation
	{
		private readonly string collection;
		private readonly Func<RavenJObject, string> generateKey;

		public RowToDatabase(
			string collection,
            Func<RavenJObject, string> generateKey)
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
                      new RavenJObject(row.Cast<KeyValuePair<string,RavenJToken>>()
							.Select(x => new KeyValuePair<string, RavenJToken>(x.Key, RavenJToken.FromObject(x.Value)))));
                                
				var putCommandDatas = jsons.Select(document => new PutCommandData
				{
					Document = document,
					Metadata = new JObject(new JProperty("Raven-Entity-Name", new JValue(collection))),
					Key = generateKey(document)
				}).ToArray();

				count++;
				File.WriteAllText(Path.Combine("Docs", collection + " #" + count.ToString("00000") + ".json"),
								  new JArray(putCommandDatas.Select(x => x.ToJson())).ToString(Formatting.Indented));

			}
			yield break;
		}
	}
}
