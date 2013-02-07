using System.Collections.Generic;
using System.Reflection;
using Android.App;
using Android.Preferences;
using Android.Widget;
using Android.OS;
using System.Linq;
using Raven.Tests.MonoForAndroid.Models;
using Raven.Tests.MonoForAndroid.Resources;

namespace Raven.Tests.MonoForAndroid
{
	[Activity(Label = "RavenDB Mono For Android Tester", Icon = "@drawable/icon")]
	public class TestsSelectionActivity : ListActivity
	{
		int count = 1;

		protected override void OnCreate(Bundle bundle)
		{
			base.OnCreate(bundle);

			ListAdapter = new TestListAdapter(this, new List<TestItem>
			{
				new TestItem{Name = "Test1", Selected = true},
				new TestItem{Name = "Test2", Selected = false},
				new TestItem{Name = "Test3", Selected = true},
			});
			
			// Set our view from the "main" layout resource
			//SetContentView(Resource.Layout.TestItem);

		
		}
	}
}

