using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;

namespace Raven.Tests.MonoForAndroid.Resources
{
	class TestListAdapter : BaseAdapter<string>
	{
		private readonly Activity context;
		public List<string> Tests;

		public TestListAdapter(Activity context, List<string> tests ): base()
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

			//if (convertView == null || !(convertView is LinearLayout))
			//	view = context.LayoutInflater.Inflate(Resource.Layout.AnimalItem, parent, false);

			////Find references to each subview in the list item's view
			//var imageItem = view.FindViewById(Resource.Id.imageItem) as ImageView;
			//var textTop = view.FindViewById(Resource.Id.textTop) as TextView;
			//var textBottom = view.FindViewById(Resource.Id.textBottom) as TextView;

			////Assign this item's values to the various subviews
			//imageItem.SetImageResource(item.Image);
			//textTop.SetText(item.Name, TextView.BufferType.Normal);
			//textBottom.SetText(item.Description, TextView.BufferType.Normal);

			//Finally return the view
			return view;
		}

		public override int Count
		{
			get { throw new NotImplementedException(); }
		}

		public override string this[int position]
		{
			get { throw new NotImplementedException(); }
		}
	}
}