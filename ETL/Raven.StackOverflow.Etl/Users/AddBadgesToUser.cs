//-----------------------------------------------------------------------
// <copyright file="AddBadgesToUser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using System.Linq;

namespace Raven.StackOverflow.Etl.Users
{
	public class AddBadgesToUser : AbstractOperation
	{
		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			int count = 0;
			foreach (var badgesForUsers in rows
				.GroupBy(row => row["UserId"])
				.Partition(Constants.BatchSize))
			{
				var cmds = new List<ICommandData>();
				foreach (var badgesForUser in badgesForUsers)
				{
					var badgesByName = new Dictionary<string, RavenJObject>();	
					var badgesArray = badgesForUser.ToArray();
					foreach (var row in badgesArray)
					{
						RavenJObject badge;
						if (badgesByName.TryGetValue((string)row["Name"], out badge )==false)
						{
							badge = new RavenJObject(new []
							{
								new KeyValuePair<string, RavenJToken>("Name", RavenJToken.FromObject(row["Name"])),     
								new KeyValuePair<string, RavenJToken>("Dates", new RavenJArray()), 
							});
							badgesByName.Add((string)row["Name"], badge);
						}
						((RavenJArray)badge["Dates"]).Add(new RavenJValue(row["Date"]));
					}

					cmds.Add(new PatchCommandData
					{
						Key = "users/" + badgesForUser.Key,
						Patches = new[]
						{
							new PatchRequest
							{
								Name = "Badges",
								Type = PatchCommandType.Set,
								Value = new RavenJArray(badgesByName.Values)
							},
						}
					});
				}

				count++;

				File.WriteAllText(Path.Combine("Docs", "Badges #" + count.ToString("00000") + ".json"),
					"[" + cmds.Select(c => c.ToJson() + ",") + "]");
			}
			yield break;
		}
	}
}
