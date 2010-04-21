using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;

namespace Raven.StackOverflow.Etl.Posts
{
	public class AddVotesToPost : AbstractOperation
	{
		private readonly DocumentDatabase database;

		public AddVotesToPost(DocumentDatabase database)
		{
			this.database = database;
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			foreach (var votesForPosts in rows
				.GroupBy(row => row["PostId"])
				.Partition(100))
			{
				var cmds = new List<ICommandData>();
				foreach (var votesForPost in votesForPosts)
				{
					var jsonDocument = database.Get("posts/" + votesForPost.Key, null);
					if (jsonDocument == null)
						continue;
					var doc = JObject.Parse(Encoding.UTF8.GetString(jsonDocument.Data));
					var votes = doc["votes"] as JArray;
					if (votes == null)
						doc["votes"] = votes = new JArray();

					var votesArray = votesForPost.ToArray();
					foreach (var row in votesArray)
					{
						var vote = new JObject(
							new JProperty("VoteTypeId", new JValue(row["VoteTypeId"])),
							new JProperty("CreationDate", new JValue(row["CreationDate"]))
							);
						switch ((long) row["VoteTypeId"])
						{
							case 5L:
								vote.Add("UserId", new JValue("users/" + row["UserId"]));
								break;
							case 9L:
								vote.Add("BountyAmount", new JValue(row["BountyAmount"]));
								break;
						}
						votes.Add(vote);
					}
					Statistics.AddOutputRows(votesArray.Length);

					cmds.Add(new PutCommandData
					{
						Document = doc,
						Etag = jsonDocument.Etag,
						Key = jsonDocument.Key,
						Metadata = jsonDocument.Metadata,
					});
				}

				database.Batch(cmds);

				Notice("Updated {0:#,#} posts votes", Statistics.OutputtedRows);
			}
			yield break;
		}
	}
}