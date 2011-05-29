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
using Raven.Abstractions.Data;
using Raven.Json;
using Raven.Json.Utilities;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;

namespace Raven.StackOverflow.Etl.Posts
{
	public class AddVotesToPost : BatchFileWritingProcess
	{
		public AddVotesToPost(string outputDirectory) : base(outputDirectory)
		{
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			int count = 0;
			foreach (var votesForPosts in rows.GroupBy(row => row["PostId"]).Partition(Constants.BatchSize))
			{
				List<RavenJToken> votes = new List<RavenJToken>();

				var cmds = new List<ICommandData>();
				foreach (var votesForPost in votesForPosts)
				{
					foreach (var row in votesForPost)
					{
						var vote = new RavenJObject(new[]
							{
								new KeyValuePair<string, RavenJToken>("VoteTypeId", new RavenJValue(row["VoteTypeId"])),
								new KeyValuePair<string, RavenJToken>("CreationDate", new RavenJValue(row["CreationDate"]))
							});

						switch ((long)row["VoteTypeId"])
						{
							case 5L:
								vote.Add("UserId", new RavenJValue("users/" + row["UserId"]));
								break;
							case 9L:
								vote.Add("BountyAmount", new RavenJValue(row["BountyAmount"]));
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
									Type = PatchCommandType.Set,
									Value = new RavenJArray( votes)
								},
							}
					});
				}

				count++;

				WriteCommandsTo("Votes #" + count.ToString("00000") + ".json", cmds);
			}
			yield break;
		}
	}
}
