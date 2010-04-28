namespace Raven.Database.Plugins
{
	public interface IDeleteTrigger
	{
		VetoResult AllowDelete(string key);
		void OnDelete(string key);
		void AfterCommit(string key);
	}
}