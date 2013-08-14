namespace Raven.Abstractions.Data
{
	public class ScriptedIndexResults
	{
		public const string IdPrefix = "Raven/ScriptedIndexResults/";

		public string Id { get; set; }
		public string IndexScript { get; set; }
		public string DeleteScript { get; set; }

		protected bool Equals(ScriptedIndexResults other)
		{
			return string.Equals(Id, other.Id) && string.Equals(IndexScript, other.IndexScript) && string.Equals(DeleteScript, other.DeleteScript);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((ScriptedIndexResults) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (Id != null ? Id.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (IndexScript != null ? IndexScript.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (DeleteScript != null ? DeleteScript.GetHashCode() : 0);
				return hashCode;
			}
		}
	}
}