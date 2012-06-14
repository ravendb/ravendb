using System;
using System.Collections.Generic;
using System.IO.IsolatedStorage;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public enum DocumentDisplayStyle
    {
        Details,
        Card
    }

	public class DocumentSize : NotifyPropertyChangedBase
	{
		public const double DefaultDocumentHeight = 130;
		public const double ExpandedDocumentHeight = 130;
		public const double CardMinimumHeight = 90;
	    public const double CardMaximumHeight = 700;

	    private double indicatorPosition;
	    private DocumentDisplayStyle displayStyle;
        private double height;
        private double width;

		private readonly static DocumentSize current = new DocumentSize {Height = DefaultDocumentHeight};
		public static DocumentSize Current
		{
			get { return current; }
		}

		public event EventHandler SizeChanged;

	    public readonly double MinimumIndicatorPosition = 0;
	    public readonly double MaximumIndicatorPosition = 100;
	    private const double DetailsToCardSwitchover = 20;
	    private const string IndicatorPostitionSettingsKey = "DocumentSize.IndicatorPosition";

	    public DocumentSize()
        {
            IndicatorPosition = DetailsToCardSwitchover;
        }

	    public double IndicatorPosition
	    {
	        get { return indicatorPosition; }
            set
            {
                if (indicatorPosition == value)
                {
                    return;
                }

                indicatorPosition = value;

                if (indicatorPosition < DetailsToCardSwitchover/2)
                {
                    indicatorPosition = 0;
                }
                else if (indicatorPosition < DetailsToCardSwitchover)
                {
                    indicatorPosition = DetailsToCardSwitchover;
                }

                UpdateHeightWidthAndDisplayStyle();
                OnPropertyChanged(() => IndicatorPosition);
            }
	    }

	    private void UpdateHeightWidthAndDisplayStyle()
	    {
            if (indicatorPosition < DetailsToCardSwitchover)
	        {
	            DisplayStyle = DocumentDisplayStyle.Details;
	        }
            else
	        {
	            DisplayStyle = DocumentDisplayStyle.Card;
	        }

            if (DisplayStyle == DocumentDisplayStyle.Card)
            {
                var cardScale = (indicatorPosition - DetailsToCardSwitchover) / (MaximumIndicatorPosition - DetailsToCardSwitchover);
                Height = CardMinimumHeight + (CardMaximumHeight - CardMinimumHeight)*cardScale;
            }
	    }

	    public double Height
		{
			get { return height; }
			set
			{
				if (height == value)
					return;
				height = value;
				OnPropertyChanged(() => Height);
				SetWidthBasedOnHeight();
			}
		}

		public double Width
		{
			get { return width; }
			private set
			{
				width = value;
				OnPropertyChanged(() => Width);
			}
		}

        public DocumentDisplayStyle DisplayStyle
        {
            get { return displayStyle; }
            private set
            {
                displayStyle = value;
                OnPropertyChanged(() => DisplayStyle);
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

	    public void LoadDefaults(IDictionary<string, object> settingsDictionary)
	    {
	        if (settingsDictionary.ContainsKey(IndicatorPostitionSettingsKey))
	        {
                IndicatorPosition = Convert.ToInt32(settingsDictionary[IndicatorPostitionSettingsKey]);
	        }
	    }

        public void SaveDefaults(IDictionary<string, object> settingsDictionary)
        {
            settingsDictionary[IndicatorPostitionSettingsKey] = IndicatorPosition;
        }
	}
}