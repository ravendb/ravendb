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
using ActiproSoftware.Text.Searching;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Primitives;

namespace Raven.Studio.Controls
{
	public class SearchControlView : EditorSearchView
	{
		public SearchControlView() : base()
		{
			this.SearchOptions = new EditorSearchOptions
			{
				MatchCase = false,
			};
		}
	}
}
