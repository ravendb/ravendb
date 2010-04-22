using System;
using System.IO;
using Raven.Database;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Users
{
	public class BadgesProcess : EtlProcess
	{
		private readonly string path;
		private readonly DocumentDatabase database;

		public BadgesProcess(string path, DocumentDatabase database)
		{
			this.path = path;
			this.database = database;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter(); 
			Register(new XmlRowOperationFile(Path.Combine(path, "badges.xml")));
			Register(new AddBadgesToUser(database));
		}
	}
}