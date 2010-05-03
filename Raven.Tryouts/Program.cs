using System;
using System.IO;
using System.Threading;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Tryouts
{
	internal class Program
	{
	

		public static void Main()
		{
			
			try
			{
				//if(Directory.Exists("bak"))
				//    Directory.Delete("bak", true);


				DocumentDatabase.Restore("bak", "Data4");
				var ravenConfiguration = new RavenConfiguration
				{
					DataDirectory = @"Data3"
				};
				using (var db = new DocumentDatabase(ravenConfiguration))
				{
					db.StartBackup("bak");
					while(true)
					{
						var jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
						if (jsonDocument == null)
							break;
						var backupStatus = jsonDocument.Data.JsonDeserialization<BackupStatus>();
						Console.Clear();
						Console.WriteLine("Backup started at {0}", backupStatus.Started);
						foreach (var message in backupStatus.Messages)
						{
							Console.WriteLine(" - {0}: {1}", message.Timestamp, message.Message);
						}
						if (backupStatus.IsRunning == false)
							return;
						Thread.Sleep(500);
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}

}