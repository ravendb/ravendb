using System.Collections.Generic;
using System.Windows;
using System.Windows.Media;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class CollectionModel : NotifyPropertyChangedBase
	{
		Brush fill;
		public virtual Brush Fill
		{
			get { return fill ?? (fill = TemplateColorProvider.Instance.ColorFrom(Name)); }
		}

		private string name;
		public string Name
		{
			get { return name; }
			set { name = value; OnPropertyChanged(() => Name);}
		}

        private int count;
		public int Count
		{
			get { return count; }
			set { count = value; OnPropertyChanged(() => Count);}
		}

	    public virtual string DisplayName
	    {
	        get { return Name; }
	    }

        public virtual long SortableCount
	    {
	        get { return Count; }
	    }
	}
	public class RavenDocumentsCollectionModel : CollectionModel
	{
		List<object> ravenDocs;

		public RavenDocumentsCollectionModel()
		{
			Name = "0";
		}

		public override long SortableCount
		{
			get
			{
				return int.MaxValue - 1;
			}
		}

		public override string DisplayName
		{
			get { return "System Documents"; }
		}

		public override Brush Fill
		{
			get
			{
				return new SolidColorBrush(Colors.Blue);
			}
		}
	}

    public class AllDocumentsCollectionModel : CollectionModel
    {
        public AllDocumentsCollectionModel()
        {
            Name = "";
		}

        public override Brush Fill
        {
            get
            {
                return CreateRainbowBrush();
            }
        }

        public static LinearGradientBrush CreateRainbowBrush()
        {
            var brush = new LinearGradientBrush { StartPoint = new Point(0, 0), EndPoint = new Point(0, 1) };
            brush.GradientStops.Add(new GradientStop() { Color = Colors.Red,  Offset = 0.00});
            brush.GradientStops.Add(new GradientStop() { Color =Colors.Orange,Offset = 0.17});
            brush.GradientStops.Add(new GradientStop() { Color =Colors.Yellow,Offset = 0.33});
            brush.GradientStops.Add(new GradientStop() { Color =Colors.Green, Offset =0.50});
            brush.GradientStops.Add(new GradientStop() { Color =Colors.Blue, Offset =0.67});
            brush.GradientStops.Add(new GradientStop() { Color = Color.FromArgb(255,75,0,130),Offset = 0.84});
            brush.GradientStops.Add(new GradientStop() { Color = Color.FromArgb(255, 143, 0, 255), Offset = 1.00 });
            return brush;
        }

        public override string DisplayName
        {
            get
            {
                return "All Documents";
            }
        }

        public override long SortableCount
        {
            get { return int.MaxValue; }
        }
    }
}