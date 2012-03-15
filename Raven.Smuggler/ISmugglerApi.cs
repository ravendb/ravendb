namespace Raven.Smuggler
{
	public interface ISmugglerApi
	{
		void ExportData(SmugglerOptions options);
		void ImportData(SmugglerOptions options);
	}
}