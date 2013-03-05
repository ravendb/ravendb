using System;

using Android.App;
using Android.Content;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using Android.OS;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;

namespace Raven.Tryouts.MonoForAndroid
{
	[Activity(Label = "Raven.Tryouts.MonoForAndroid", MainLauncher = true, Icon = "@drawable/icon")]
	public class Activity1 : Activity, IDisposable
	{
		int count = 1;

		private IDocumentStore Store { get; set; }


		protected override void OnCreate(Bundle bundle)
		{
			base.OnCreate(bundle);

			Store = new DocumentStore { DefaultDatabase = "Mono", Url = "http://192.168.1.12:8080" }.Initialize();


			// Set our view from the "main" layout resource
			SetContentView(Resource.Layout.Main);

			// Get our button from the layout resource,
			// and attach an event to it
			Button button = FindViewById<Button>(Resource.Id.MyButton);
			Store.Changes("Mono").ForAllDocuments().Subscribe(obj => RunOnUiThread(() =>
			{
				var local = FindViewById<Button>(Resource.Id.MyButton);
				local.Text = "changed at: " + DateTime.Now.TimeOfDay;
			}));

			button.Click += delegate { button.Text = string.Format("{0} clicks!", count++); };
		}

		protected override void Dispose(bool disposing)
		{
			base.Dispose(disposing);
			Store.Dispose();
		}
	}
}

