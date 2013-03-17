using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Client.Connection;

namespace Raven.Studio.Models
{
	public class UserInfoModel : PageViewModel
	{
		public UserInfo UserInfo { get; set; }
		public Dictionary<string, string> UserData { get; set; }

		public UserInfoModel()
		{
			ModelUrl = "/user-info";
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

			return value.ToString();
		}
	}
}
