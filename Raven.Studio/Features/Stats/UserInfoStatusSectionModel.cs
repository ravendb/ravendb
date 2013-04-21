using System.Collections.Generic;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Client.Connection;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Features.Stats
{
	public class UserInfoStatusSectionModel : StatusSectionModel
	{
		public UserInfo UserInfo { get; set; }
		public Dictionary<string, string> UserData { get; set; }

		public UserInfoStatusSectionModel()
		{
			SectionName = "User Info";
			UserData = new Dictionary<string, string>();
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value
			                .AsyncDatabaseCommands
			                .CreateRequest(string.Format("/debug/user-info").NoCache(), "GET")
			                .ReadResponseJsonAsync()
			                .ContinueOnSuccessInTheUIThread(doc =>
			                {
								UserInfo = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer()
												   .Deserialize<UserInfo>(new RavenJTokenReader(doc));
				                UpdateInfo();
			                });
		}

		private void UpdateInfo()
		{
			UserData.Clear();

			foreach (var propertyInfo in UserInfo.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
			{
				var property = propertyInfo.GetValue(UserInfo, null);
				if (property == null || string.IsNullOrEmpty(property.ToString()) || property.ToString() == "0")
					continue;

				UserData.Add(propertyInfo.Name, GetValueWithFormat(propertyInfo.GetValue(UserInfo, null)));

			}

			UserData = new Dictionary<string, string>(UserData);
			OnPropertyChanged(() => UserData);
		}

		private string GetValueWithFormat(object value)
		{
			if (value == null)
				return null;

			if (value is int)
				return ((int)value).ToString("#,#");
			if (value is double)
				return ((double)value).ToString("#,#");
			if (value is long)
				return ((long)value).ToString("#,#");
			if (value is float)
				return ((float)value).ToString("#,#");
			var enumerable = value as IEnumerable<string>;
			if (enumerable != null)
			{
				var items = enumerable.ToList();
				return items.Count == 0 ? null : string.Join(", ", items);
			}
			var databases = value as List<DatabaseInfo>;
			if (databases != null)
			{
				var names = databases.Select(info => info.Database).ToList();
				return names.Count == 0 ? null : string.Join(", ", names);
			}

			return value.ToString();
		}
	}
}
