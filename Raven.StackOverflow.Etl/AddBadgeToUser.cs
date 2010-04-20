using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using System.Linq;

namespace Raven.StackOverflow.Etl
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
			foreach (var badgesForUser in rows.GroupBy(row => row["UserId"]))
			{
				var jsonDocument = database.Get("users/" + badgesForUser.Key,null);
				var doc = JObject.Parse(Encoding.UTF8.GetString(jsonDocument.Data));
				var badges = doc["badges"] as JArray;
				if (badges == null)
					doc["badges"] = badges = new JArray();

				foreach (var row in badgesForUser)
				{
					badges.Add(new JObject(
						new JProperty("Name", new JValue(row["Name"])),
						new JProperty("Date", new JValue(row["Date"]))
						));	
				}

				database.Put(jsonDocument.Key, jsonDocument.Etag, doc, jsonDocument.Metadata, null);
			}
			yield break;
		}
	}
}