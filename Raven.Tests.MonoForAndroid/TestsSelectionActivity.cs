using System.Reflection;
using Android.App;
using Android.Preferences;
using Android.Widget;
using Android.OS;
using System.Linq;

namespace Raven.Tests.MonoForAndroid
{
	[Activity(Label = "Raven.Tests.MonoForAndroid", MainLauncher = true, Icon = "@drawable/icon")]
	public class TestsSelectionActivity : PreferenceActivity
	{
		int count = 1;

		protected override void OnCreate(Bundle bundle)
		{
			base.OnCreate(bundle);

			// Set our view from the "main" layout resource
			SetContentView(Resource.Layout.TestSelection);

			// Get our button from the layout resource,
			// and attach an event to it

			//AddPreferencesFromResource();
			var items = Assembly.GetCallingAssembly().GetTypes();

			//foreach (var item in items)
			//{
			//	var methods = item.GetMethods();
			//	foreach (var methodInfo in methods)
			//	{
			//		var list = methodInfo.GetCustomAttributes(typeof(FactAttribute), true);
					

			//	}
			//}

			Button button = FindViewById<Button>(Resource.Id.MyButton);
			//var testsList = FindViewById<ListView>(Resource.Id.TestsList);
			//testsList.ChoiceMode = ChoiceMode.Multiple;
			//testsList.Adapter = new ArrayAdapter<string>(this, Resource.Layout.Main, MonoForAndroidTestBase.Tests.Keys.ToArray());

			button.Click += delegate { button.Text = string.Format("{0} clicks!", count++); };
		}
	}
}

