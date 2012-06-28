using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using System.Linq;
using Raven.Studio.Extensions;

namespace Raven.Studio.Controls
{
    public partial class ViewSizeControl : UserControl
    {
        public static readonly DependencyProperty ViewSizeProperty =
            DependencyProperty.Register("ViewSize", typeof (double), typeof (ViewSizeControl), new PropertyMetadata(default(double)));

        private static readonly List<PresetViewSize> ViewSizes;
        
        public double ViewSize
        {
            get { return (double) GetValue(ViewSizeProperty); }
            set { SetValue(ViewSizeProperty, value); }
        }

        static ViewSizeControl()
        {
            DetailsViewSize = new PresetViewSize() {Key = "Details", Size = 0};
            MediumCardViewSize = new PresetViewSize() {Key = "MediumCard", Size = 45};

            ViewSizes = new List<PresetViewSize>()
                            {
                                DetailsViewSize,
                                new PresetViewSize() {Key = "SmallCard", Size = 10},
                                MediumCardViewSize,
                                new PresetViewSize() {Key = "LargeCard", Size = 75},
                                new PresetViewSize() {Key = "ExtraLargeCard", Size = 100},
                            };
        }

        public ViewSizeControl()
        {
            InitializeComponent();
            

            LayoutRoot.DataContext = this;

        }

        private ICommand setViewSize;
        private ICommand toggleViewSize;
        private static PresetViewSize DetailsViewSize;
        private static PresetViewSize MediumCardViewSize;

        public ICommand SetViewSize
        {
            get { return setViewSize ?? (setViewSize = new ActionCommand(HandleSetViewSize)); }
        }

        public ICommand ToggleViewSize
        {
            get { return toggleViewSize ?? (toggleViewSize = new ActionCommand(HandleToggleViewSize)); }
        }

        private void HandleToggleViewSize()
        {
              if (ViewSize > DetailsViewSize.Size)
              {
                  SetViewSize.Execute(DetailsViewSize.Key);
              }
              else
              {
                  SetViewSize.Execute(MediumCardViewSize.Key);
              }
        }

        private void HandleSetViewSize(object parameter)
        {
            var sizeKey = parameter as string;
            if (string.IsNullOrEmpty(sizeKey))
            {
                return;
            }

            var size = ViewSizes.FirstOrDefault(s => s.Key == sizeKey);
            if (size != null)
            {
                ViewSize = size.Size;
            }
        }

        private class PresetViewSize
        {
            public string Key { get; set; }
            public double Size { get; set; }
        }

    }
}
