using System;
using System.IO;
using Raven.Database;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Posts
{
	public class CommentsProcess : EtlProcess
	{
		private readonly string path;
		private readonly DocumentDatabase database;

		public CommentsProcess(string path, DocumentDatabase database)
		{
			this.path = path;
			this.database = database;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter();
			Register(new XmlRowOperationFile(Path.Combine(path, "comments.xml")));
			Register(new AddCommentsToPost(database));
		}
	}
}