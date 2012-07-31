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
            IdOnlyViewSize = new PresetViewSize() {Key = "IdOnly", Size = 20};
            MediumCardViewSize = new PresetViewSize() {Key = "MediumCard", Size = 60};

            ViewSizes = new List<PresetViewSize>()
                            {
                                DetailsViewSize,
                                IdOnlyViewSize,
                                new PresetViewSize() {Key = "SmallCard", Size = 40},
                                MediumCardViewSize,
                                new PresetViewSize() {Key = "LargeCard", Size = 80},
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
        private static PresetViewSize IdOnlyViewSize;

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
              if (ViewSize > IdOnlyViewSize.Size)
              {
                  SetViewSizeFromPreset(DetailsViewSize);
              }
              else if (ViewSize > DetailsViewSize.Size || ViewSize.IsCloseTo(IdOnlyViewSize.Size))
              {
                  SetViewSizeFromPreset(MediumCardViewSize);
              }
              else
              {
                  SetViewSizeFromPreset(IdOnlyViewSize);
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
            SetViewSizeFromPreset(size);
        }

        private void SetViewSizeFromPreset(PresetViewSize size)
        {
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
