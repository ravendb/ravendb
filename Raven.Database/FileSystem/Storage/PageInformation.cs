namespace Raven.Database.Server.RavenFS.Storage
{
	public class PageInformation
	{
		public int Id { get; set; }
		public int Size { get; set; }

		public override bool Equals(object obj)
		{
			if (obj == null || GetType() != obj.GetType())
				return false;

			var page = obj as PageInformation;

			if (page == null)
				return false;

			return page.Id == Id && page.Size == Size;
		}

		public override int GetHashCode()
		{
			return Id.GetHashCode() ^ Size.GetHashCode();
		}
	}
}
