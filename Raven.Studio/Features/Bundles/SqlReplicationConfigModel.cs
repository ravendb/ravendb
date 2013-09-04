using System.Collections.Generic;
using System.Collections.ObjectModel;
using Raven.Database.Bundles.SqlReplication;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Bundles
{
	public class SqlReplicationConfigModel : ViewModel
	{
		protected bool Equals(SqlReplicationConfigModel other)
		{
			var baseEquals = string.Equals(Name, other.Name) && string.Equals(Script, other.Script) &&
			       string.Equals(FactoryName, other.FactoryName) && string.Equals(ConnectionString, other.ConnectionString) &&
			       string.Equals(ConnectionStringName, other.ConnectionStringName) &&
			       string.Equals(ConnectionStringSettingName, other.ConnectionStringSettingName) && string.Equals(Id, other.Id) &&
			       string.Equals(RavenEntityName, other.RavenEntityName) && Disabled.Equals(other.Disabled) &&
			       Equals(SqlReplicationTables.Count, other.SqlReplicationTables.Count);
			if (baseEquals == false)
				return false;

			for (int i = 0; i < SqlReplicationTables.Count; i++)
			{
				if (SqlReplicationTables[i].Equals(other.SqlReplicationTables[i]) == false)
					return false;
			}

			return true;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((SqlReplicationConfigModel) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (name != null ? name.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (script != null ? script.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (factoryName != null ? factoryName.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (connectionString != null ? connectionString.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (connectionStringName != null ? connectionStringName.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (connectionStringSettingName != null ? connectionStringSettingName.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (id != null ? id.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (ravenEntityName != null ? ravenEntityName.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ disabled.GetHashCode();
				hashCode = (hashCode*397) ^ (sqlReplicationTables != null ? sqlReplicationTables.GetHashCode() : 0);
				return hashCode;
			}
		}

		private string name;
		private string script;
		private string factoryName;
		private string connectionString;
		private string connectionStringName;
		private string connectionStringSettingName;
		private string id;
		private string ravenEntityName;
		private bool disabled;
		private ObservableCollection<SqlReplicationTable> sqlReplicationTables;

		public SqlReplicationConfigModel()
		{
			sqlReplicationTables = new ObservableCollection<SqlReplicationTable>();
		}

		public string Id
		{
			get { return id; }
			set
			{
				id = value;
				OnPropertyChanged(() => Id);
			}

		}

		public string Name
		{
			get { return name; }
			set
			{
				name = value;
				OnPropertyChanged(() => Name);
			}
		}

		public string RavenEntityName
		{
			get { return ravenEntityName; }
			set
			{
				ravenEntityName = value;
				OnPropertyChanged(() => RavenEntityName);
			}
		}

		public string Script
		{
			get { return script; }
			set
			{
				script = value;
				OnPropertyChanged(() => Script);
			}
		}
		public string FactoryName
		{
			get { return factoryName; }
			set
			{
				factoryName = value;
				OnPropertyChanged(() => FactoryName);
			}
		}

		public string ConnectionString
		{
			get { return connectionString; }
			set
			{
				connectionString = value;
				OnPropertyChanged(() => ConnectionString);
			}
		}

		public string ConnectionStringName
		{
			get { return connectionStringName; }
			set
			{
				connectionStringName = value;
				OnPropertyChanged(() => ConnectionStringName);
			}
		}
		public string ConnectionStringSettingName
		{
			get { return connectionStringSettingName; }
			set
			{
				connectionStringSettingName = value;
				OnPropertyChanged(() => ConnectionStringSettingName);
			}
		}

		public bool Disabled
		{
			get { return disabled; }
			set
			{
				disabled = value;
				OnPropertyChanged(() => Disabled);
			}
		}

		public ObservableCollection<SqlReplicationTable> SqlReplicationTables
		{
			get { return sqlReplicationTables; }
			set
			{
				sqlReplicationTables = value;
				OnPropertyChanged(() => SqlReplicationTables);
			}
		}

		public SqlReplicationConfig ToSqlReplicationConfig()
		{
			var result = new SqlReplicationConfig
			{
				Id = Id,
				Name = Name,
				FactoryName = FactoryName,
				RavenEntityName = RavenEntityName,
				Script = Script,
				Disabled = disabled,
				SqlReplicationTables = new List<SqlReplicationTable>(sqlReplicationTables)
			};

			if (string.IsNullOrWhiteSpace(ConnectionString) == false)
				result.ConnectionString = ConnectionString;
			else if (string.IsNullOrWhiteSpace(ConnectionStringName) == false)
				result.ConnectionStringName = connectionStringName;
			else
				result.ConnectionStringSettingName = connectionStringSettingName;

			return result;
		}

		public static SqlReplicationConfigModel FromSqlReplicationConfig(SqlReplicationConfig config)
		{
			return new SqlReplicationConfigModel
			{
				Id = config.Id,
				Name = config.Name,
				FactoryName = config.FactoryName,
				RavenEntityName = config.RavenEntityName,
				Script = config.Script,
				ConnectionString = config.ConnectionString,
				ConnectionStringName = config.ConnectionStringName,
				ConnectionStringSettingName = config.ConnectionStringSettingName,
				Disabled = config.Disabled,
				SqlReplicationTables = new ObservableCollection<SqlReplicationTable>(config.SqlReplicationTables)
			};
		}
	}
}
