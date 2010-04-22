using System;
using System.IO;
using Raven.Database;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Posts
{
	public class PostsProcess : EtlProcess
	{
		private readonly string path;
		private readonly DocumentDatabase database;

		public PostsProcess(string path, DocumentDatabase database)
		{
			this.path = path;
			this.database = database;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter();
			Register(new XmlRowOperationFile(Path.Combine(path, "posts.xml")));
			Register(new RowToDatabase(database, "Posts", doc => "posts/" + doc["Id"]));
		}
	}
}