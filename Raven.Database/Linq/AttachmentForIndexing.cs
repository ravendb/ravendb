namespace Raven.Database.Linq
{
	public class AttachmentForIndexing 
	{
		private readonly string key;
		public string Key
		{
			get { return key; }
		}

		public AttachmentForIndexing(string key)
		{
			this.key = key;
		}
	}
}