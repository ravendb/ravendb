namespace Raven.Abstractions.Database.Smuggler.Database
{
    public class DatabaseSmugglerOperationState : DatabaseLastEtagsInfo
	{
        public string FilePath { get; set; }
	}
}