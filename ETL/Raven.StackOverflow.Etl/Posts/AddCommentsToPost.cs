using System;
using System.Collections.Generic;
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
	public class AddCommentsToPost : AbstractOperation
	{
		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			int count = 0;
			foreach (var commentsForPosts in rows.GroupBy(row => row["PostId"]).Partition(Constants.BatchSize))
			{
				var cmds = new List<ICommandData>();

				foreach (var commentsForPost in commentsForPosts)
				{
					var comments = new JArray();
					foreach (var row in commentsForPost)
					{
						comments.Add(new JObject(
						             	new JProperty("Score", new JValue(row["Score"])),
						             	new JProperty("CreationDate", new JValue(row["CreationDate"])),
						             	new JProperty("Text", new JValue(row["Text"])),
						             	new JProperty("UserId", new JValue("users/" + row["UserId"]))
						             	));

					}
					cmds.Add(new PatchCommandData
					{
						Key = "posts/" + commentsForPost.Key,
						Patches = new[]
						{
							new PatchRequest
							{
								Name = "Comments",
								Type = "Set",
								Value = comments
							},
						}
					});
				}

				count++;

				File.WriteAllText(Path.Combine("Docs", "Comments #" + count.ToString("00000") + ".json"),
								  new JArray(cmds.Select(x => x.ToJson())).ToString(Formatting.Indented));

				
			}

			yield break;
		}
	}
}
