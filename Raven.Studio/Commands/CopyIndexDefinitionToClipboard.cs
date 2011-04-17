namespace Raven.Studio.Commands
{
	using System.Windows;
	using Database.Indexing;
	using Newtonsoft.Json;

	public class CopyIndexDefinitionToClipboard
	{
		public void Execute(IndexDefinition index)
		{
			var json = JsonConvert.SerializeObject(index, Formatting.Indented);
			Clipboard.SetText(json);
		}

		public bool CanExecute(IndexDefinition index)
		{
			return index != null;
		}
	}
}