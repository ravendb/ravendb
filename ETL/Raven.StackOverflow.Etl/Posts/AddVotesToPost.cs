//-----------------------------------------------------------------------
// <copyright file="AddVotesToPost.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions.Commands;
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
			foreach (var votesForPosts in rows.GroupBy(row => row["PostId"]).Partition(Constants.BatchSize))
			{
				var cmds = new List<ICommandData>();
				foreach (var votesForPost in votesForPosts)
				{
					var votes = new JArray();
					foreach (var row in votesForPost)
					{
						var vote = new JObject(
							new JProperty("VoteTypeId", new JValue(row["VoteTypeId"])),
							new JProperty("CreationDate", new JValue(row["CreationDate"]))
							);
						switch ((long)row["VoteTypeId"])
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
					cmds.Add(new PatchCommandData
					{
						Key = "posts/" + votesForPost.Key,
						Patches = new[]
							{
								new PatchRequest
								{
									Name = "Votes",
									Type = "Set",
									Value = votes
								},
							}
					});
				}

				count++;

				File.WriteAllText(Path.Combine("Docs", "Votes #" + count.ToString("00000") + ".json"),
								  new JArray(cmds.Select(x => x.ToJson())).ToString(Formatting.Indented));

			}
			yield break;
		}
	}
}
