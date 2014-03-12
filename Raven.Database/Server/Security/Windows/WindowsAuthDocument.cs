using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsAuthDocument
	{
		public List<WindowsAuthData> RequiredGroups { get; set; }
		public List<WindowsAuthData> RequiredUsers { get; set; }

		public WindowsAuthDocument()
		{
			RequiredGroups = new List<WindowsAuthData>();
			RequiredUsers = new List<WindowsAuthData>();
		}
	}

	public class WindowsAuthData
	{
		public string Name { get; set; }
		public bool Enabled { get; set; }
		public List<DatabaseAccess> Databases { get; set; }
        public List<FileSystemAccess> FileSystems { get; set; } 

		protected bool Equals(WindowsAuthData other)
		{
		    var baseEqual = string.Equals(Name, other.Name) && Enabled.Equals(other.Enabled) &&
                Equals(Databases.Count, other.Databases.Count) && Equals(FileSystems.Count, other.FileSystems.Count);

			if(baseEqual == false)
				return false;

			for (int i = 0; i < Databases.Count; i++)
			{
				if(Databases[i].Equals(other.Databases[i]) == false)
					return false;
			}

            for (int i = 0; i < FileSystems.Count; i++)
            {
                if (FileSystems[i].Equals(other.FileSystems[i]) == false)
                    return false;
            }

			return true;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((WindowsAuthData) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (Name != null ? Name.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ Enabled.GetHashCode();
				hashCode = (hashCode*397) ^ (Databases != null ? Databases.GetHashCode() : 0);
			    hashCode = (hashCode*397) ^ (FileSystems != null ? FileSystems.GetHashCode() : 0);
				return hashCode;
			}
		}

		public WindowsAuthData()
		{
			Databases = new List<DatabaseAccess>();
            FileSystems = new List<FileSystemAccess>();
		}
	}
}