using System;
using System.Collections.Generic;
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
	public class AddCommentsToPost : AbstractOperation
	{
		private readonly DocumentDatabase database;

		public AddCommentsToPost(DocumentDatabase database)
		{
			this.database = database;
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			foreach (var commentsForPosts in rows
				.GroupBy(row => row["PostId"])
				.Partition(100))
			{
				var cmds = new List<ICommandData>();
				foreach (var commentForPost in commentsForPosts)
				{
					var jsonDocument = database.Get("posts/" + commentForPost.Key, null);
					if(jsonDocument == null)
						continue;
					var doc = JObject.Parse(Encoding.UTF8.GetString(jsonDocument.Data));
					var comments = doc["comments"] as JArray;
					if (comments == null)
						doc["comments"] = comments = new JArray();

					var commentsArray = commentForPost.ToArray();
					foreach (var row in commentsArray)
					{
						var comment = new JObject(
							new JProperty("Score", new JValue(row["Score"])),
							new JProperty("CreationDate", new JValue(row["CreationDate"])),
							new JProperty("Text", new JValue(row["Text"])),
							new JProperty("UserId", new JValue("users/" + row["UserId"]))
							);
						
						comments.Add(comment);
					}
					Statistics.AddOutputRows(commentsArray.Length);

					cmds.Add(new PutCommandData
					{
						Document = doc,
						Etag = jsonDocument.Etag,
						Key = jsonDocument.Key,
						Metadata = jsonDocument.Metadata,
					});
				}

				database.Batch(cmds);

				Notice("Updated {0:#,#} posts comments", Statistics.OutputtedRows);
			}
			yield break;
		}
	}
}