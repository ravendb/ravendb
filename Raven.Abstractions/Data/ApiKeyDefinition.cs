using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
	public class ApiKeyDefinition : INotifyPropertyChanged
	{
		public string Id { get; set; }

		private string name;
		public string Name
		{
			get { return name; }
			set
			{
				name = value;
				OnPropertyChanged("Name");
				OnPropertyChanged("FullApiKey");
				OnPropertyChanged("ConnectionString");
			}
		}

		private string secret;
		public string Secret
		{
			get { return secret; }
			set
			{
				secret = value;
				OnPropertyChanged("Secret");
				OnPropertyChanged("FullApiKey");
				OnPropertyChanged("ConnectionString");
			}
		}

		[JsonIgnore]
		public string FullApiKey
		{
			get
			{
				if (string.IsNullOrWhiteSpace(Name) || string.IsNullOrWhiteSpace(Secret))
					return "Must set both name and secret to get the full api key";

				return Name + "/" + Secret;
			}
		}

		[JsonIgnore]
		public string ConnectionString
		{
			get
			{
				if (string.IsNullOrWhiteSpace(Name) || string.IsNullOrWhiteSpace(Secret))
					return null;

				return string.Format(@"ApiKey = {0}; Database = {1}", FullApiKey, DbName);
			}
		}

		[JsonIgnore]
		private string DbName
		{
			get 
			{ 
				var access = Databases.FirstOrDefault();
				return access == null ? "DbName" : access.TenantId;
			}
		}

		public bool Enabled { get; set; }

		public List<DatabaseAccess> Databases { get; set; }

	    public ApiKeyDefinition()
	    {
	        Databases = new List<DatabaseAccess>();
	    }

		public event PropertyChangedEventHandler PropertyChanged;

		protected virtual void OnPropertyChanged(string propertyName)
		{
			PropertyChangedEventHandler handler = PropertyChanged;
			if (handler != null) handler(this, new PropertyChangedEventArgs(propertyName));
		}

		protected bool Equals(ApiKeyDefinition other)
		{
			var baseEqual =  string.Equals(Id, other.Id) && Enabled.Equals(other.Enabled) && Equals(Databases.Count, other.Databases.Count) &&
			       string.Equals(Secret, other.Secret) && string.Equals(Name, other.Name);

			if(baseEqual == false)
				return false;

			for (int i = 0; i < Databases.Count; i++)
			{
				if (Databases[i].Equals(other.Databases[i]) == false)
					return false;
			}

			return true;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((ApiKeyDefinition) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (Id != null ? Id.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ Enabled.GetHashCode();
				hashCode = (hashCode*397) ^ (Databases != null ? Databases.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (secret != null ? secret.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (name != null ? name.GetHashCode() : 0);
				return hashCode;
			}
		}
	}

	public class DatabaseAccess
	{
		public bool Admin { get; set; }
		public string TenantId { get; set; }
		public bool ReadOnly { get; set; }

		protected bool Equals(DatabaseAccess other)
		{
			return Admin.Equals(other.Admin) && string.Equals(TenantId, other.TenantId) && ReadOnly.Equals(other.ReadOnly);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((DatabaseAccess) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = Admin.GetHashCode();
				hashCode = (hashCode*397) ^ (TenantId != null ? TenantId.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ ReadOnly.GetHashCode();
				return hashCode;
			}
		}
	}
}