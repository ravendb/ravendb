using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Android.App;
using Android.Graphics;
using Android.OS;
using Android.Widget;
using Java.Lang;
using NUnit.Framework;
using Raven.Tests.MonoForAndroid.Models;
using Raven.Tests.MonoForAndroid.Resources;
using System.Linq;
using Exception = System.Exception;

namespace Raven.Tests.MonoForAndroid
{
	[Activity(Label = "RavenDB Mono For Android Tester", MainLauncher = true, Icon = "@drawable/icon")]
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

			var tests = GetAllTests();
			MarkAllAs(tests, true);

			list.Adapter = new TestListAdapter(this, tests);

			selectButton.Click += (sender, args) =>
			{
				MarkAllAs(tests, true);

				list.Adapter = new TestListAdapter(this, tests);
			};

			deselectButton.Click += (sender, args) =>
			{
				MarkAllAs(tests, false);

				list.Adapter = new TestListAdapter(this, tests);
			};

			runTestsButton.Click += (sender, args) =>
			{
				SetContentView(Resource.Layout.Testing);
				var selectedTests = tests.Where(item => item.Selected).ToList();
				var thread = new Thread(() => RunTests(selectedTests));
				thread.Run();
			};
		}

		private List<TestItem> GetAllTests()
		{
			return Assembly.GetExecutingAssembly().GetTypes()
			               .Where(type => type.IsSubclassOf(typeof (MonoForAndroidTestBase)))
			               .SelectMany(testType => testType.GetMethods())
			               .Where(method => method.GetCustomAttributes(typeof (TestAttribute), true).Length > 0)
			               .Select(testMethod => new TestItem
			               {
				               Name = testMethod.DeclaringType.Name + "->" + testMethod.Name,
							   Action = () =>
							   {
								   var instance = Activator.CreateInstance(testMethod.DeclaringType);
								   try
								   {
									   return testMethod.Invoke(instance, null);  
								   }
								   finally
								   {
									   var disposable = instance as IDisposable;
									   if(disposable != null)
										   disposable.Dispose();
								   }
							   }
			               })
			               .ToList();
		}

		private int passed = 0;
		private int failed = 0;
		public void RunTests(List<TestItem> testsToRun)
		{

			var failedTitle = FindViewById<TextView>(Resource.Id.FailedTests);
			failedTitle.SetTextColor(Color.Red);

			var passedTitle = FindViewById<TextView>(Resource.Id.PassedTests);
			passedTitle.SetTextColor(Color.Green);
			
			var failedTestsList = FindViewById<TextView>(Resource.Id.FailedTestsList);
			var passedTestsList = FindViewById<TextView>(Resource.Id.PassedTestsList);
			var testsStatus = FindViewById<TextView>(Resource.Id.TestsStatus);


			failedTestsList.Text = "";
			passedTestsList.Text = "";

			RunTest(testsToRun);
		}
		int index = -1;
		private void RunTest(List<TestItem> testsToRun)
		{
			var failedTestsList = FindViewById<TextView>(Resource.Id.FailedTestsList);
			var passedTestsList = FindViewById<TextView>(Resource.Id.PassedTestsList);
			var testsStatus = FindViewById<TextView>(Resource.Id.TestsStatus);
			index++;
			if (testsToRun.Count > index)
			{
				UpdateStatus(testsStatus, passed, failed, testsToRun.Count());

				try
				{
					var result = testsToRun[index].Action.Invoke();
					var task = result as Task;
					if (task != null)
					{
						var item = testsToRun[index];
						task.ContinueWith(task1 =>
						{
							if (task.IsFaulted)
							{
								RunOnUiThread(() =>
								{
									failedTestsList.Text += item.Name + "\nError: " + task.Exception + "\n\n";
									failed++;
								});
							}
							else
							{
								RunOnUiThread(() =>
								{
									passedTestsList.Text += item.Name + "\n";
									passed++;
								});
							}

							RunTest(testsToRun);
						});
					}
					else
					{
						RunOnUiThread(() =>
						{
							passedTestsList.Text += testsToRun[index].Name + "\n";
							passed++;
							UpdateStatus(testsStatus, passed, failed, testsToRun.Count());				
							RunTest(testsToRun);
						});
					}

				}
				catch (Exception e)
				{
					RunOnUiThread(() =>
					{
						failedTestsList.Text += testsToRun[index].Name + "\nError: " + e + "\n\n";
						failed++;
						UpdateStatus(testsStatus, passed, failed, testsToRun.Count());				
						RunTest(testsToRun);
					});
				}
			}
			else
			{
				UpdateStatus(testsStatus, passed, failed, testsToRun.Count());				
			}
		}

		private void UpdateStatus(TextView testsStatus, int passed, int failed, int count)
		{
			RunOnUiThread(() =>
			{
				testsStatus.Text = string.Format("Passed: {0}, Failed: {1}, of {2}", passed, failed, count);				
			});
		}

		private static void MarkAllAs(IEnumerable<TestItem> tests, bool value)
		{
			foreach (var testItem in tests)
			{
				testItem.Selected = value;
			}
		}
	}
}