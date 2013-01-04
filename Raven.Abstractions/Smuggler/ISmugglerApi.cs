namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
		string ExportData(SmugglerOptions options, bool incremental);
		void ImportData(SmugglerOptions options, bool incremental);
	}
}