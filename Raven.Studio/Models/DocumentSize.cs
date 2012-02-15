using System;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DocumentSize : NotifyPropertyChangedBase
	{
		public const double DefaultDocumentHeight = 66;
		public const double ExpandedDocumentHeight = 130;
		public const double ExpandedMinimumHeight = 110;
		
		private readonly static DocumentSize current = new DocumentSize {Height = DefaultDocumentHeight};
		public static DocumentSize Current
		{
			get { return current; }
		}

		public event EventHandler SizeChanged;
		
		private double height;
		public double Height
		{
			get { return height; }
			set
			{
				if (height == value)
					return;
				height = value;
				OnPropertyChanged();
				SetWidthBasedOnHeight();
			}
		}

		private double width;
		public double Width
		{
			get { return width; }
			set
			{
				width = value;
				OnPropertyChanged();
			}
		}

		private void SetWidthBasedOnHeight()
		{
			const double wideAspectRatio = 1.7;
			const double narrowAspectRatio = 0.707;
			const double aspectRatioSwitchoverHeight = 120;
			const double wideRatioMaxWidth = aspectRatioSwitchoverHeight*wideAspectRatio;
			const double narrowAspectRatioSwitchoverHeight = wideRatioMaxWidth/narrowAspectRatio;

			Width = Height < aspectRatioSwitchoverHeight ? Height*wideAspectRatio
			        	: Height < narrowAspectRatioSwitchoverHeight ? wideRatioMaxWidth
			        	  	: Height*narrowAspectRatio;

			if (SizeChanged != null)
				SizeChanged(this, EventArgs.Empty);
		}
	}
}