using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Json;
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
					var badgesByName = new Dictionary<string, JObject>();	
					var badgesArray = badgesForUser.ToArray();
					foreach (var row in badgesArray)
					{
						JObject badge;
						if (badgesByName.TryGetValue((string)row["Name"], out badge )==false)
						{
							badge = new JObject(
								new JProperty("Name", new JValue(row["Name"])),
								new JProperty("Dates", new JArray())
								);
							badgesByName.Add((string)row["Name"], badge);
						}
						((JArray)badge["Dates"]).Add(new JValue(row["Date"]));
					}

					cmds.Add(new PatchCommandData()
					{
						Key = "users/" + badgesForUser.Key,
						Patches = badgesByName.Values.Select(o => new PatchRequest
						{
							Name = "Badges",
							Type = "Add",
							Value = o
						}).ToArray()
					});
				}

				count++;

				File.WriteAllText(Path.Combine("Docs", "Badges #" + count.ToString("00000") + ".json"),
								  new JArray(cmds.Select(x=>x.ToJson())).ToString(Formatting.Indented));

			}
			yield break;
		}
	}
}