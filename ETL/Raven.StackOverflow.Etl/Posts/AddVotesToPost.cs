using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;

namespace Raven.StackOverflow.Etl.Posts
{
	public class AddVotesToPost : AbstractOperation
	{
		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			int count = 0;
			foreach (var votesForPosts in rows.Partition(Constants.BatchSize))
			{
				var cmds = new List<ICommandData>();
				foreach (var row in votesForPosts)
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

					cmds.Add(new PatchCommandData()
					{
						Key = "posts/" + row["PostId"],
						Patches = new[]
						{
							new PatchRequest
							{
								Name = "Votes",
								Type = "Add",
								Value = vote
							},
						}
					});
				}

				count++;

				File.WriteAllText(Path.Combine("Docs", "Votes #" + count + ".json"),
								  new JArray(cmds.Select(x => x.ToJson())).ToString(Formatting.Indented));

			}
			yield break;
		}
	}
}