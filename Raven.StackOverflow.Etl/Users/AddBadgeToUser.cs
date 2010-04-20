using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using System.Linq;

namespace Raven.StackOverflow.Etl.Users
{
	public class AddBadgeToUser : AbstractOperation
	{
		private readonly DocumentDatabase database;

		public AddBadgeToUser(DocumentDatabase database)
		{
			this.database = database;
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			foreach (var badgesForUsers in rows
				.GroupBy(row => row["UserId"])
				.Partition(100))
			{
				var cmds = new List<ICommandData>();
				foreach (var badgesForUser in badgesForUsers)
				{
					var jsonDocument = database.Get("users/" + badgesForUser.Key, null);
					if(jsonDocument == null)
						continue;
					var doc = JObject.Parse(Encoding.UTF8.GetString(jsonDocument.Data));
					var badges = doc["badges"] as JArray;
					if (badges == null)
						doc["badges"] = badges = new JArray();

					var badgesArray = badgesForUser.ToArray();
					foreach (var row in badgesArray)
					{
						var badge = badges.FirstOrDefault(x => x["Name"].Value<string>() == (string)row["Name"]);
						if (badge == null)
						{
							badge = new JObject(
								new JProperty("Name", new JValue(row["Name"])),
								new JProperty("Dates", new JArray())
								);
							badges.Add(badge);
						}
						((JArray)badge["Dates"]).Add(new JValue(row["Date"]));
					}
					Statistics.AddOutputRows(badgesArray.Length);

					cmds.Add(new PutCommandData
					{
						Document = doc,
						Etag = jsonDocument.Etag,
						Key = jsonDocument.Key,
						Metadata = jsonDocument.Metadata,
					});
				}

				database.Batch(cmds);

				Notice("Updated {0:#,#} users badges", Statistics.OutputtedRows);
			}
			yield break;
		}
	}
}