namespace Raven.Abstractions.Smuggler.Data
{
    public class OperationState : LastEtagsInfo
	{
        public string FilePath { get; set; }
	}
}