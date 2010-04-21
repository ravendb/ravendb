using System;
using System.IO;
using Raven.Database;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;

namespace Raven.StackOverflow.Etl.Users
{
	public class UsersProcess : EtlProcess
	{
		private readonly string path;
		private readonly DocumentDatabase database;

		public UsersProcess(string path, DocumentDatabase database)
		{
			this.path = path;
			this.database = database;
		}

		protected override void Initialize()
		{
			Register(new XmlRowOperationFile(Path.Combine(path, "users.xml")));
			Register(new TryConvert<DateTime>(DateTime.TryParse));
			Register(new TryConvert<long>(long.TryParse));
			Register(new RowToDatabase(database, "Users", doc => "users/" + doc["Id"]));
		}
	}
}