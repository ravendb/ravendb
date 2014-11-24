namespace Raven.Abstractions.Smuggler.Data
{
	public class ExportDataResult : LastEtagsInfo
	{
        public string FilePath { get; set; }
	}
}