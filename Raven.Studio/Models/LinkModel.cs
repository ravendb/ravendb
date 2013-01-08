namespace Raven.Studio.Models
{
	public class LinkModel
	{
		public string Title { get; set; }
		public string HRef { get; set; }

		public LinkModel()
		{
			
		}

		public LinkModel(string title)
		{
			Title = title;
			HRef = "/edit?id=" + title;
		}
	}
}