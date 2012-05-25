namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
		void ExportData(SmugglerOptions options, bool incremental);
		void ImportData(SmugglerOptions options, bool incremental);
	}
}