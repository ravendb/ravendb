namespace Raven.Abstractions.Data
{
	public class ScriptedIndexResults
	{
		public const string IdPrefix = "Raven/ScriptedIndexResults/";

		public string Id { get; set; }
		public string IndexScript { get; set; }
		public string DeleteScript { get; set; }
	}
}