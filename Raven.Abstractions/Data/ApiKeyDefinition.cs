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

				return string.Format(@"ApiKey = {0}, Database = {1}", FullApiKey, DbName);
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

	}

	public class DatabaseAccess
	{
		public bool Admin { get; set; }
		public string TenantId { get; set; }
		public bool ReadOnly { get; set; }
	}
}