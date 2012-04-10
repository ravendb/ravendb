namespace Raven.Smuggler
{
	public interface ISmugglerApi
	{
		void ExportData(SmugglerOptions options, bool incremental);
		void ImportData(SmugglerOptions options);
	}
}