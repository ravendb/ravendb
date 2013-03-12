using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Database.Bundles.SqlReplication;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Bundles
{
    public class SqlReplicationConfigModel : ViewModel
    {
        private string name;
        private string script;
        private string factoryName;
        private string connectionString;
        private string connectionStringName;
        private string connectionStringSettingName;
        private string id;
        private string ravenEntityName;

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

		public SqlReplicationConfig ToSqlReplicationConfig()
		{
			var result = new SqlReplicationConfig
			{
				Id = Id,
				Name = Name,
				FactoryName = FactoryName,
				RavenEntityName = RavenEntityName,
				Script = Script
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
				ConnectionStringSettingName = config.ConnectionStringSettingName
			};
		}
    }
}
