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
			foreach (var commentsForPosts in rows.Partition(100))
			{
				var cmds = new List<ICommandData>(
					commentsForPosts.Select(row => new PatchCommandData()
					{
						Key = "posts/" + row["PostId"],
						Patches = new[]
						{
							new PatchRequest
							{
								Name = "Comments",
								Type = "Add",
								Value = new JObject(
									new JProperty("Score", new JValue(row["Score"])),
									new JProperty("CreationDate", new JValue(row["CreationDate"])),
									new JProperty("Text", new JValue(row["Text"])),
									new JProperty("UserId", new JValue("users/" + row["UserId"]))
									)
							},
						}
					}));
				
				count++;

				File.WriteAllText(Path.Combine("Docs", "Comments #" + count + ".json"),
								  new JArray(cmds.Select(x => x.ToJson())).ToString(Formatting.Indented));

				
			}

			yield break;
		}
	}
}