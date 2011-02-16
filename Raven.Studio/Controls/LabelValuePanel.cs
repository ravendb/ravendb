namespace Raven.Studio.Controls
{
	using System.Linq;
	using System.Windows;
	using System.Windows.Controls;
	using Caliburn.Micro;

	public class LabelValuePanel : Grid
	{
		public LabelValuePanel()
		{
			ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
			ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(6) });
			ColumnDefinitions.Add(new ColumnDefinition());

			Loaded += delegate
							{
								Enumerable
									.Range(1, (Children.Count / 2))
									.Apply(x => RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto }));

								for (var index = 0; index < Children.Count; index++)
								{
									var row = index/2;
									var col = (index%2 == 1) ? 2 : 0;

									var child = (FrameworkElement)Children[index];

									SetColumn(child, col);
									SetRow(child,row);
								}
							};
		}
	}
}