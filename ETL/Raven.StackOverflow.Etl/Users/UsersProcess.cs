using System;
using System.IO;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Users
{
	public class UsersProcess : EtlProcess
	{
		private readonly string path;

		public UsersProcess(string path)
		{
			this.path = path;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter();
			Register(new XmlRowOperationFile(Path.Combine(path, "users.xml")));
			Register(new RowToDatabase("Users", doc => "users/" + doc["Id"]));
		}
	}
}