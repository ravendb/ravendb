using System.Collections.Generic;
using Android.App;
using Android.Views;
using Android.Widget;
using Raven.Tests.MonoForAndroid.Models;
using System.Linq;

namespace Raven.Tests.MonoForAndroid.Resources
{
	class TestListAdapter : BaseAdapter<TestItem>
	{
		private readonly Activity context;
		public List<TestItem> Tests;

		public TestListAdapter(Activity context, List<TestItem> tests ): base()
		{
			this.context = context;
			Tests = tests;
		}

		public override long GetItemId(int position)
		{
			return position;
		}

		public override View GetView(int position, View convertView, ViewGroup parent)
		{
			var item = Tests[position];

			var view = convertView;

			if (convertView == null || !(convertView is LinearLayout))
				view = context.LayoutInflater.Inflate(Resource.Layout.TestItem, parent, false);

			//Find references to each subview in the list item's view
			var checkBox = view.FindViewById(Resource.Id.checkBox1) as CheckBox;
		
			//Assign this item's values to the various subviews
			checkBox.Checked = item.Selected;
			checkBox.Text = item.Name;
			checkBox.Click += (sender, args) =>
			{
				var box = ((CheckBox)sender);
				var name = box.Text;

				foreach (var testItem in Tests.Where(testItem => testItem.Name == name))
				{
					testItem.Selected = box.Checked;
				}
			};

			//checkBox.SetText(item.Name, TextView.BufferType.Normal);
			//Finally return the view
			return view;
		}

		public override int Count
		{
			get { return Tests.Count; }
		}

		public override TestItem this[int position]
		{
			get { return Tests[position]; }
		}
	}
}