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
using Raven.StackOverflow.Etl.Posts;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using System.Linq;
using System;


namespace Raven.StackOverflow.Etl.Generic
{
	public class RowToDatabase : BatchFileWritingProcess
	{
		private readonly string collection;
		private readonly Func<RavenJObject, string> generateKey;

		public RowToDatabase(
			string collection,
			Func<RavenJObject, string> generateKey, 
			string outputDirectory)
			: base(outputDirectory)
		{
			this.collection = collection;
			this.generateKey = generateKey;
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			int count = 0;
			foreach (var partitionedRows in rows.Partition(Constants.BatchSize))
			{
				List<PutCommandData> commands = new List<PutCommandData>();

				foreach(var row in partitionedRows)
				{
					RavenJObject obj = new RavenJObject();
					
					foreach(object key in row.Keys)
					{
						obj.Add((string)key, RavenJToken.FromObject(row[key]));
					}
					 
					commands.Add(new PutCommandData()
					{
						Document = obj,
						Metadata = new RavenJObject(new[]
					{
						new KeyValuePair<string, RavenJToken>("Raven-Entity-Name", new RavenJValue(collection)), }),
						Key = generateKey(obj)
					});
				}
								
				count++;
				WriteCommandsTo(collection + " #" + count.ToString("00000") + ".json", commands);

			}
			yield break;
		}
	}
}
