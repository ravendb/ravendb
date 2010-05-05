namespace Raven.Database.Backup
{
	/// <summary>
	/// Delete the backup status document, since it may indicate a backup was in progress when the server crashed / shutdown
	/// </summary>
	public class RemoveBackupDocumentStartupTask : IStartupTask
	{
		public void Execute(DocumentDatabase database)
		{
			database.Delete("Raven/Backup/Status", null, null);
		}
	}
}