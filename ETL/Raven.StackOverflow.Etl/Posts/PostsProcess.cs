using System;
using System.IO;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Posts
{
	public class PostsProcess : EtlProcess
	{
		private readonly string path;

		public PostsProcess(string path)
		{
			this.path = path;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter();
			Register(new XmlRowOperationFile(Path.Combine(path, "posts.xml")));
			Register(new RowToDatabase("Posts", doc => "posts/" + doc["Id"]));
		}
	}
}
