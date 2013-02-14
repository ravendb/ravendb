using System;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	[AttributeUsage(AttributeTargets.All)]
	public class DescriptionAttribute : Attribute
	{
		public DescriptionAttribute(string description)
		{
			Description = description;
		}

		public string Description { get; private set; }

		public override bool Equals(object obj)
		{
			if (obj == this)
				return true;

			var other = obj as DescriptionAttribute;
			return other != null && other.Description == Description;
		}

		public override int GetHashCode()
		{
			return Description.GetHashCode();
		}
	}
}