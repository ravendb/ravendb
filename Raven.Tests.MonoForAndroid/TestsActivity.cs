using System.Collections.Generic;
using Android.App;
using Android.OS;
using Android.Widget;
using Raven.Tests.MonoForAndroid.Models;
using Raven.Tests.MonoForAndroid.Resources;
using System.Linq;

namespace Raven.Tests.MonoForAndroid
{
	[Activity(Label = "Raven.Tests.MonoForAndroid", MainLauncher = true, Icon = "@drawable/icon")]
	public class TestsActivity : Activity
	{
		protected override void OnCreate(Bundle bundle)
		{
			base.OnCreate(bundle);
			
			SetContentView(Resource.Layout.Main);

			var list = FindViewById<ListView>(Resource.Id.TestsList);
			var selectButton = FindViewById<Button>(Resource.Id.SelectAllButton);
			var deselectButton = FindViewById<Button>(Resource.Id.DeselectAllButton);
			var runTestsButton = FindViewById<Button>(Resource.Id.RunTests);
			var tests = new List<TestItem>();
			for (int i = 0; i < 20; i++)
			{
				tests.Add(new TestItem{Name = "Test" + (i + 1), Selected = true});
			}

			list.Adapter = new TestListAdapter(this, tests);

			selectButton.Click += (sender, args) =>
			{
				foreach (var testItem in tests)
				{
					testItem.Selected = true;
				}

				list.Adapter = new TestListAdapter(this, tests);
			};

			deselectButton.Click += (sender, args) =>
			{
				foreach (var testItem in tests)
				{
					testItem.Selected = false;
				}

				list.Adapter = new TestListAdapter(this, tests);
			};

			runTestsButton.Click += (sender, args) =>
			{
				foreach (var testItem in tests.Where(item => item.Selected))
				{
					//TODO: run the tests
				}
			};
		}
	}
}