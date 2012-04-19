using System;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Controls
{
    public class VirtualizingWrapPanel : VirtualizingPanel, IScrollInfo
    {
        private const double ScrollLineAmount = 16.0;

        private Size _extentSize;
        private Size _viewportSize;
        private Point _offset;
        private ItemsControl _itemsControl;
        private readonly Dictionary<UIElement, Rect> _childLayouts = new Dictionary<UIElement, Rect>();
 
        public static readonly DependencyProperty ItemWidthProperty =
            DependencyProperty.Register("ItemWidth", typeof (double), typeof (VirtualizingWrapPanel), new PropertyMetadata(1.0, HandleItemDimensionChanged));

        public static readonly DependencyProperty ItemHeightProperty =
            DependencyProperty.Register("ItemHeight", typeof(double), typeof(VirtualizingWrapPanel), new PropertyMetadata(1.0, HandleItemDimensionChanged));

        private static readonly DependencyProperty VirtualItemIndexProperty =
            DependencyProperty.RegisterAttached("VirtualItemIndex", typeof(int), typeof(VirtualizingWrapPanel), new PropertyMetadata(-1));

        private static int GetVirtualItemIndex(DependencyObject obj)
        {
            return (int)obj.GetValue(VirtualItemIndexProperty);
        }

        private static void SetVirtualItemIndex(DependencyObject obj, int value)
        {
            obj.SetValue(VirtualItemIndexProperty, value);
        }

        public double ItemHeight
        {
            get { return (double) GetValue(ItemHeightProperty); }
            set { SetValue(ItemHeightProperty, value); }
        }

        public double ItemWidth
        {
            get { return (double) GetValue(ItemWidthProperty); }
            set { SetValue(ItemWidthProperty, value); }
        }

        public VirtualizingWrapPanel()
        {
            Dispatcher.BeginInvoke(Initialize);
        }

        private void Initialize()
        {
            _itemsControl = ItemsControl.GetItemsOwner(this);
        }

        protected override void OnItemsChanged(object sender, ItemsChangedEventArgs args)
        {
            base.OnItemsChanged(sender, args);

            InvalidateMeasure();
        }

        protected override Size MeasureOverride(Size availableSize)
        {
            if (_itemsControl == null || _itemsControl.Items.Count == 0)
            {
                return availableSize;
            }

            _childLayouts.Clear();

            var layoutInfo = GetLayoutInfo(availableSize, ItemHeight);

            RecycleItems(layoutInfo);

            // Determine where the first item is in relation to previously realized items
            var generatorStartPosition = ItemContainerGenerator.GeneratorPositionFromIndex(layoutInfo.FirstRealizedItemIndex);

            // Determine where we should be inserting new items in our children collection
            // .Index refers to the last realized item
            // .Offset tells us how far away the new item is from that
            // But we don't need to leave gaps in our children collection, so 
            // if the Offset is non-zero we just move to the next index
            var visualIndex = 0;

            var itemCount = _itemsControl.Items.Count;
            var itemIndex = layoutInfo.FirstRealizedItemIndex;

            var currentX = layoutInfo.FirstRealizedItemLeft;
            var currentY = layoutInfo.FirstRealizedItemTop;

            var viewportBottom = _offset.Y + availableSize.Height;

            using (ItemContainerGenerator.StartAt(generatorStartPosition, GeneratorDirection.Forward, true))
            {
                while (itemIndex < itemCount && currentY < viewportBottom)
                {
                    bool newlyRealized;

                    var child = (UIElement)ItemContainerGenerator.GenerateNext(out newlyRealized);
                    SetVirtualItemIndex(child, itemIndex);

                    if (newlyRealized || visualIndex >= Children.Count)
                    {
                        InsertInternalChild(visualIndex, child);
                    }
                    else
                    {
                        // check if item is recycled and needs to be moved into a new position in the Children collection
                        if (Children[visualIndex] != child)
                        {
                            var childCurrentIndex = Children.IndexOf(child);

                            if (childCurrentIndex >= 0)
                            {
                                RemoveInternalChildRange(childCurrentIndex, 1);
                            }

                            InsertInternalChild(visualIndex, child);
                        }
                    }

                    // only prepare the item once it has been added to the visual tree
                    ItemContainerGenerator.PrepareItemContainer(child);

                    child.Measure(new Size(ItemWidth, ItemHeight));
                 
                    _childLayouts.Add(child, new Rect(currentX, currentY, child.DesiredSize.Width, child.DesiredSize.Height));

                    if (currentX + ItemWidth * 2 >= availableSize.Width)
                    {
                        // wrap to a new line
                        currentY += ItemHeight;
                        currentX = 0;
                    }
                    else
                    {
                        currentX += ItemWidth;
                    }

                    visualIndex++;
                    itemIndex++;
                }
            }

            RemoveRedundantChildren();
            UpdateScrollInfo(availableSize, layoutInfo);

            return availableSize;
        }

        private void RecycleItems(ItemLayoutInfo layoutInfo)
        {
            foreach (var child in Children)
            {
                var virtualItemIndex = GetVirtualItemIndex(child);
                
                if (virtualItemIndex < layoutInfo.FirstRealizedItemIndex || virtualItemIndex > layoutInfo.LastRealizedItemIndex)
                {
                    var generatorPosition = ItemContainerGenerator.GeneratorPositionFromIndex(virtualItemIndex);
                    ((IRecyclingItemContainerGenerator)ItemContainerGenerator).Recycle(generatorPosition, 1);
                    SetVirtualItemIndex(child, -1);
                }
            }
        }

        protected override Size ArrangeOverride(Size finalSize)
        {
            foreach (var child in Children)
            {
                child.Arrange(_childLayouts[child]);
            }

            return finalSize;
        }

        private void UpdateScrollInfo(Size availableSize, ItemLayoutInfo layoutInfo)
        {
            _viewportSize = availableSize;
            _extentSize = new Size(availableSize.Width, Math.Max(layoutInfo.TotalLines * ItemHeight, _viewportSize.Height));
            var verticalOffset = Clamp(_offset.Y, 0, _extentSize.Height - _viewportSize.Height);
            _offset = new Point(_offset.X, verticalOffset);

            InvalidateScrollInfo();
        }

        private void RemoveRedundantChildren()
        {
            // iterate backwards through the child collection because we're going to be
            // removing items from it
            for (var i = Children.Count - 1; i >= 0; i--)
            {
                var child = Children[i];

                // if the virtual item index is -1, this indicates
                // it is a recycled item that hasn't been reused this time round
                if (GetVirtualItemIndex(child) == -1)
                {
                    RemoveInternalChildRange(i, 1);
                }
            }
        }

        private ItemLayoutInfo GetLayoutInfo(Size availableSize, double itemHeight)
        {
            var itemsPerLine = Math.Max((int)Math.Floor(availableSize.Width/ItemWidth),1);
            var precedingLines = (int) Math.Floor(HorizontalOffset/itemHeight);
            var firstRealizedIndex = itemsPerLine*precedingLines;
            var realizedLines = (int) Math.Ceiling(availableSize.Height/itemHeight);
            var lastRealizedIndex = firstRealizedIndex + realizedLines*itemsPerLine - 1;
            var totalLines = (int) Math.Ceiling((double)_itemsControl.Items.Count/itemsPerLine);
            
            return new ItemLayoutInfo
                       {
                           ItemsPerLine = itemsPerLine,
                           TotalLines = totalLines,
                           FirstRealizedItemIndex = firstRealizedIndex,
                           FirstRealizedItemLeft = -HorizontalOffset,
                           FirstRealizedItemTop = precedingLines*itemHeight - VerticalOffset,
                           LastRealizedItemIndex = lastRealizedIndex,
                       };
        }

        public void LineUp()
        {
            SetVerticalOffset(VerticalOffset - ScrollLineAmount);
        }

        public void LineDown()
        {
            SetVerticalOffset(VerticalOffset + ScrollLineAmount);
        }

        public void LineLeft()
        {
            SetHorizontalOffset(HorizontalOffset + ScrollLineAmount);
        }

        public void LineRight()
        {
            SetHorizontalOffset(HorizontalOffset - ScrollLineAmount);
        }

        public void PageUp()
        {
            SetVerticalOffset(VerticalOffset - ViewportHeight);
        }

        public void PageDown()
        {
            SetVerticalOffset(VerticalOffset + ViewportHeight);
        }

        public void PageLeft()
        {
            SetHorizontalOffset(HorizontalOffset + ItemWidth);
        }

        public void PageRight()
        {
            SetHorizontalOffset(HorizontalOffset - ItemWidth);
        }

        public void MouseWheelUp()
        {
            SetVerticalOffset(VerticalOffset - ScrollLineAmount * SystemParameters.WheelScrollLines);
        }

        public void MouseWheelDown()
        {
            SetVerticalOffset(VerticalOffset + ScrollLineAmount * SystemParameters.WheelScrollLines);
        }

        public void MouseWheelLeft()
        {
            SetHorizontalOffset(HorizontalOffset - ScrollLineAmount * SystemParameters.WheelScrollLines);
        }

        public void MouseWheelRight()
        {
            SetHorizontalOffset(HorizontalOffset + ScrollLineAmount * SystemParameters.WheelScrollLines);
        }

        public void SetHorizontalOffset(double offset)
        {
            offset = Clamp(offset, 0, ExtentWidth - ViewportWidth);
            _offset = new Point(offset, _offset.Y);

            InvalidateMeasure();
        }

        public void SetVerticalOffset(double offset)
        {
            offset = Clamp(offset, 0, ExtentHeight - ViewportHeight);
            _offset = new Point(_offset.X, offset);

            InvalidateMeasure();
        }

        public Rect MakeVisible(UIElement visual, Rect rectangle)
        {
            return new Rect();
        }

        public bool CanVerticallyScroll
        {
            get; set;
        }

        public bool CanHorizontallyScroll
        {
            get; set;
        }
        
        public double ExtentWidth
        {
            get { return _extentSize.Width; }
        }
        
        public double ExtentHeight
        {
            get { return _extentSize.Height; }
        }

        public double ViewportWidth
        {
            get { return _viewportSize.Width; }
        }

        public double ViewportHeight
        {
            get { return _viewportSize.Height; }
        }

        public double HorizontalOffset
        {
            get { return _offset.X; }
        }

        public double VerticalOffset
        {
            get { return _offset.Y; }
        }

        public ScrollViewer ScrollOwner
        {
            get; set;
        }

        private void InvalidateScrollInfo()
        {
            if (ScrollOwner != null)
            {
                ScrollOwner.InvalidateScrollInfo();
            }
        }

        private static void HandleItemDimensionChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var wrapPanel = (d as VirtualizingWrapPanel);

            if (e.Property == ItemHeightProperty)
            {
                wrapPanel.ScaleScrollOffsetToCompensateForItemHeightChange((double)e.NewValue);
            }

            wrapPanel.InvalidateMeasure();
        }

        private void ScaleScrollOffsetToCompensateForItemHeightChange(double newItemHeight)
        {
            if (_viewportSize.Height < 1 || _extentSize.Height < 1)
            {
                return;
            }

            var oldMaxOffset = _extentSize.Height - _viewportSize.Height;
            if (oldMaxOffset < 1)
            {
                _offset.Y = 0;
                return;
            }

            var newLayoutInfo = GetLayoutInfo(_viewportSize, newItemHeight);
            var newExtentHeight = newLayoutInfo.TotalLines*newItemHeight;
            var newMaxOffset = newExtentHeight - _viewportSize.Height;
            if (newMaxOffset < 1)
            {
                _offset.Y = 0;
                return;
            }

            var currentRelativePosition = _offset.Y/oldMaxOffset;
            var newPosition = newMaxOffset * currentRelativePosition;

            _offset.Y = newPosition;
        }

        private double Clamp(double value, double min, double max)
        {
            return Math.Min(Math.Max(value, min), max);
        }

        private class ItemLayoutInfo
        {
            public int FirstRealizedItemIndex;
            public double FirstRealizedItemTop;
            public double FirstRealizedItemLeft;
            public int ItemsPerLine;
            public int TotalLines;
            public int LastRealizedItemIndex;
        }
    }
}
