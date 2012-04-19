using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
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

namespace Raven.Studio.Impl.VirtualizingWrapPanel
{
    public class VirtualizingWrapPanel : VirtualizingPanel, IScrollInfo
    {

        #region Fields

        UIElementCollection _children;
        ItemsControl _itemsControl;
        IItemContainerGenerator _generator;
        private Point _offset = new Point(0, 0);
        private Size _extent = new Size(0, 0);
        private Size _viewport = new Size(0, 0);
        private int firstIndex = 0;
        private Size childSize;
        private Size _pixelMeasuredViewport = new Size(0, 0);
        Dictionary<UIElement, Rect> _realizedChildLayout = new Dictionary<UIElement, Rect>();
        WrapPanelAbstraction _abstractPanel;

        public VirtualizingWrapPanel()
        {
            Dispatcher.BeginInvoke(Initialize);
        }
        #endregion

        #region Properties

        private Size ChildSlotSize
        {
            get
            {
                return new Size(ItemWidth, ItemHeight);
            }
        }

        #endregion

        #region Dependency Properties

        [TypeConverter(typeof(LengthConverter))]
        public double ItemHeight
        {
            get
            {
                return (double)base.GetValue(ItemHeightProperty);
            }
            set
            {
                base.SetValue(ItemHeightProperty, value);
            }
        }

        [TypeConverter(typeof(LengthConverter))]
        public double ItemWidth
        {
            get
            {
                return (double)base.GetValue(ItemWidthProperty);
            }
            set
            {
                base.SetValue(ItemWidthProperty, value);
            }
        }

        public Orientation Orientation
        {
            get { return (Orientation)GetValue(OrientationProperty); }
            set { SetValue(OrientationProperty, value); }
        }

        public static readonly DependencyProperty ItemHeightProperty = DependencyProperty.Register("ItemHeight", typeof(double), typeof(VirtualizingWrapPanel), new PropertyMetadata(double.PositiveInfinity));
        public static readonly DependencyProperty ItemWidthProperty = DependencyProperty.Register("ItemWidth", typeof(double), typeof(VirtualizingWrapPanel), new PropertyMetadata(double.PositiveInfinity));
        public static readonly DependencyProperty OrientationProperty = DependencyProperty.Register("Orientation", typeof(Orientation), typeof(VirtualizingWrapPanel), new PropertyMetadata(Orientation.Horizontal));

        #endregion

        #region Methods

        private void SetFirstRowViewItemIndex(int index)
        {
            SetVerticalOffset((index) / Math.Floor((_viewport.Width) / childSize.Width));
            SetHorizontalOffset((index) / Math.Floor((_viewport.Height) / childSize.Height));
        }

        public void Resizing(object sender, EventArgs e)
        {
            if (_viewport.Width != 0)
            {
                int firstIndexCache = firstIndex;
                _abstractPanel = null;
                MeasureOverride(_viewport);
                SetFirstRowViewItemIndex(firstIndex);
                firstIndex = firstIndexCache;
            }
        }

        public int GetFirstVisibleSection()
        {
            int section;
            var maxSection = 0;
            if (_abstractPanel != null)
            {
                maxSection = _abstractPanel.Max(x => x.Section);
            }
            if (Orientation == Orientation.Horizontal)
            {
                section = (int)_offset.Y;
            }
            else
            {
                section = (int)_offset.X;
            }
            if (section > maxSection)
                section = maxSection;
            return section;
        }

        public int GetFirstVisibleIndex()
        {
            int section = GetFirstVisibleSection();

            if (_abstractPanel != null)
            {
                var item = _abstractPanel.Where(x => x.Section == section).FirstOrDefault();
                if (item != null)
                    return item._index;
            }
            return 0;
        }

        public void CleanUpItems(int minDesiredGenerated, int maxDesiredGenerated)
        {
            for (int i = _children.Count - 1; i >= 0; i--)
            {
                GeneratorPosition childGeneratorPos = new GeneratorPosition(i, 0);
                int itemIndex = _generator.IndexFromGeneratorPosition(childGeneratorPos);
                if (itemIndex < minDesiredGenerated || itemIndex > maxDesiredGenerated)
                {
                    _generator.Remove(childGeneratorPos, 1);
                    RemoveInternalChildRange(i, 1);
                }
            }
        }

        private void ComputeExtentAndViewport(Size pixelMeasuredViewportSize, int visibleSections)
        {
            if (Orientation == Orientation.Horizontal)
            {
                _viewport.Height = visibleSections;
                _viewport.Width = pixelMeasuredViewportSize.Width;
            }
            else
            {
                _viewport.Width = visibleSections;
                _viewport.Height = pixelMeasuredViewportSize.Height;
            }

            if (Orientation == Orientation.Horizontal)
            {
                _extent.Height = _abstractPanel.SectionCount + ViewportHeight - 1;

            }
            else
            {
                _extent.Width = _abstractPanel.SectionCount + ViewportWidth - 1;
            }
            _owner.InvalidateScrollInfo();
        }

        private void ResetScrollInfo()
        {
            _offset.X = 0;
            _offset.Y = 0;
        }

        private int GetNextSectionClosestIndex(int itemIndex)
        {
            var abstractItem = _abstractPanel[itemIndex];
            if (abstractItem.Section < _abstractPanel.SectionCount - 1)
            {
                var ret = _abstractPanel.
                    Where(x => x.Section == abstractItem.Section + 1).
                    OrderBy(x => Math.Abs(x.SectionIndex - abstractItem.SectionIndex)).
                    First();
                return ret._index;
            }
            else
                return itemIndex;
        }

        private int GetLastSectionClosestIndex(int itemIndex)
        {
            var abstractItem = _abstractPanel[itemIndex];
            if (abstractItem.Section > 0)
            {
                var ret = _abstractPanel.
                    Where(x => x.Section == abstractItem.Section - 1).
                    OrderBy(x => Math.Abs(x.SectionIndex - abstractItem.SectionIndex)).
                    First();
                return ret._index;
            }
            else
                return itemIndex;
        }

        private void NavigateDown()
        {
            var gen = _generator.GetItemContainerGeneratorForPanel(this);
            UIElement selected = GetFocusedElement();
            int itemIndex = gen.IndexFromContainer(selected);
            int depth = 0;
            while (itemIndex == -1)
            {
                selected = (UIElement)VisualTreeHelper.GetParent(selected);
                itemIndex = gen.IndexFromContainer(selected);
                depth++;
            }
            DependencyObject next = null;
            if (Orientation == Orientation.Horizontal)
            {
                int nextIndex = GetNextSectionClosestIndex(itemIndex);
                next = gen.ContainerFromIndex(nextIndex);
                while (next == null)
                {
                    SetVerticalOffset(VerticalOffset + 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(nextIndex);
                }
            }
            else
            {
                if (itemIndex == _abstractPanel._itemCount - 1)
                    return;
                next = gen.ContainerFromIndex(itemIndex + 1);
                while (next == null)
                {
                    SetHorizontalOffset(HorizontalOffset + 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(itemIndex + 1);
                }
            }
            while (depth != 0)
            {
                next = VisualTreeHelper.GetChild(next, 0);
                depth--;
            }
            (next as Control).Focus();
        }

        private void NavigateLeft()
        {
            var gen = _generator.GetItemContainerGeneratorForPanel(this);

            UIElement selected = GetFocusedElement();
            int itemIndex = gen.IndexFromContainer(selected);
            int depth = 0;
            while (itemIndex == -1)
            {
                selected = (UIElement)VisualTreeHelper.GetParent(selected);
                itemIndex = gen.IndexFromContainer(selected);
                depth++;
            }
            DependencyObject next = null;
            if (Orientation == Orientation.Vertical)
            {
                int nextIndex = GetLastSectionClosestIndex(itemIndex);
                next = gen.ContainerFromIndex(nextIndex);
                while (next == null)
                {
                    SetHorizontalOffset(HorizontalOffset - 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(nextIndex);
                }
            }
            else
            {
                if (itemIndex == 0)
                    return;
                next = gen.ContainerFromIndex(itemIndex - 1);
                while (next == null)
                {
                    SetVerticalOffset(VerticalOffset - 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(itemIndex - 1);
                }
            }
            while (depth != 0)
            {
                next = VisualTreeHelper.GetChild(next, 0);
                depth--;
            }
            (next as Control).Focus();
        }

        private UIElement GetFocusedElement()
        {
            return (UIElement)FocusManager.GetFocusedElement();
        }

        private void NavigateRight()
        {
            var gen = _generator.GetItemContainerGeneratorForPanel(this);
            UIElement selected = GetFocusedElement();
            int itemIndex = gen.IndexFromContainer(selected);
            int depth = 0;
            while (itemIndex == -1)
            {
                selected = (UIElement)VisualTreeHelper.GetParent(selected);
                itemIndex = gen.IndexFromContainer(selected);
                depth++;
            }
            DependencyObject next = null;
            if (Orientation == Orientation.Vertical)
            {
                int nextIndex = GetNextSectionClosestIndex(itemIndex);
                next = gen.ContainerFromIndex(nextIndex);
                while (next == null)
                {
                    SetHorizontalOffset(HorizontalOffset + 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(nextIndex);
                }
            }
            else
            {
                if (itemIndex == _abstractPanel._itemCount - 1)
                    return;
                next = gen.ContainerFromIndex(itemIndex + 1);
                while (next == null)
                {
                    SetVerticalOffset(VerticalOffset + 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(itemIndex + 1);
                }
            }
            while (depth != 0)
            {
                next = VisualTreeHelper.GetChild(next, 0);
                depth--;
            }
            (next as Control).Focus();
        }

        private void NavigateUp()
        {
            var gen = _generator.GetItemContainerGeneratorForPanel(this);
            UIElement selected = GetFocusedElement();
            int itemIndex = gen.IndexFromContainer(selected);
            int depth = 0;
            while (itemIndex == -1)
            {
                selected = (UIElement)VisualTreeHelper.GetParent(selected);
                itemIndex = gen.IndexFromContainer(selected);
                depth++;
            }
            DependencyObject next = null;
            if (Orientation == Orientation.Horizontal)
            {
                int nextIndex = GetLastSectionClosestIndex(itemIndex);
                next = gen.ContainerFromIndex(nextIndex);
                while (next == null)
                {
                    SetVerticalOffset(VerticalOffset - 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(nextIndex);
                }
            }
            else
            {
                if (itemIndex == 0)
                    return;
                next = gen.ContainerFromIndex(itemIndex - 1);
                while (next == null)
                {
                    SetHorizontalOffset(HorizontalOffset - 1);
                    UpdateLayout();
                    next = gen.ContainerFromIndex(itemIndex - 1);
                }
            }
            while (depth != 0)
            {
                next = VisualTreeHelper.GetChild(next, 0);
                depth--;
            }
            (next as Control).Focus();
        }


        #endregion

        #region Override

        protected void HandleKeyDown(object sender, KeyEventArgs e)
        {
            switch (e.Key)
            {
                case Key.Down:
                    NavigateDown();
                    e.Handled = true;
                    break;
                case Key.Left:
                    NavigateLeft();
                    e.Handled = true;
                    break;
                case Key.Right:
                    NavigateRight();
                    e.Handled = true;
                    break;
                case Key.Up:
                    NavigateUp();
                    e.Handled = true;
                    break;
            }
        }


        protected override void OnItemsChanged(object sender, ItemsChangedEventArgs args)
        {
            base.OnItemsChanged(sender, args);
            _abstractPanel = null;
            ResetScrollInfo();
        }

        protected void Initialize()
        {
            _itemsControl = ItemsControl.GetItemsOwner(this);
            _children = Children;
            _generator = ItemContainerGenerator;
            SizeChanged += Resizing;
            KeyDown += HandleKeyDown;
        }

        protected override Size MeasureOverride(Size availableSize)
        {
            if (_itemsControl == null || _itemsControl.Items.Count == 0)
                return availableSize;
            if (_abstractPanel == null)
                _abstractPanel = new WrapPanelAbstraction(_itemsControl.Items.Count);

            _pixelMeasuredViewport = availableSize;

            _realizedChildLayout.Clear();

            Size realizedFrameSize = availableSize;

            int itemCount = _itemsControl.Items.Count;
            int firstVisibleIndex = GetFirstVisibleIndex();

            GeneratorPosition startPos = _generator.GeneratorPositionFromIndex(firstVisibleIndex);

            int childIndex = (startPos.Offset == 0) ? startPos.Index : startPos.Index + 1;
            int current = firstVisibleIndex;
            int visibleSections = 1;
            using (_generator.StartAt(startPos, GeneratorDirection.Forward, true))
            {
                bool stop = false;
                bool isHorizontal = Orientation == Orientation.Horizontal;
                double currentX = 0;
                double currentY = 0;
                double maxItemSize = 0;
                int currentSection = GetFirstVisibleSection();
                while (current < itemCount)
                {
                    bool newlyRealized;

                    // Get or create the child                    
                    UIElement child = _generator.GenerateNext(out newlyRealized) as UIElement;
                    if (newlyRealized)
                    {
                        // Figure out if we need to insert the child at the end or somewhere in the middle
                        if (childIndex >= _children.Count)
                        {
                            base.AddInternalChild(child);
                        }
                        else
                        {
                            base.InsertInternalChild(childIndex, child);
                        }
                        _generator.PrepareItemContainer(child);
                        child.Measure(ChildSlotSize);
                    }
                    else
                    {
                        // The child has already been created, let's be sure it's in the right spot
                        Debug.Assert(child == _children[childIndex], "Wrong child was generated");
                    }
                    childSize = child.DesiredSize;
                    Rect childRect = new Rect(new Point(currentX, currentY), childSize);
                    if (isHorizontal)
                    {
                        maxItemSize = Math.Max(maxItemSize, childRect.Height);
                        if (childRect.Right > realizedFrameSize.Width) //wrap to a new line
                        {
                            currentY = currentY + maxItemSize;
                            currentX = 0;
                            maxItemSize = childRect.Height;
                            childRect.X = currentX;
                            childRect.Y = currentY;
                            currentSection++;
                            visibleSections++;
                        }
                        if (currentY > realizedFrameSize.Height)
                            stop = true;
                        currentX = childRect.Right;
                    }
                    else
                    {
                        maxItemSize = Math.Max(maxItemSize, childRect.Width);
                        if (childRect.Bottom > realizedFrameSize.Height) //wrap to a new column
                        {
                            currentX = currentX + maxItemSize;
                            currentY = 0;
                            maxItemSize = childRect.Width;
                            childRect.X = currentX;
                            childRect.Y = currentY;
                            currentSection++;
                            visibleSections++;
                        }
                        if (currentX > realizedFrameSize.Width)
                            stop = true;
                        currentY = childRect.Bottom;
                    }
                    _realizedChildLayout.Add(child, childRect);
                    _abstractPanel.SetItemSection(current, currentSection);

                    if (stop)
                        break;
                    current++;
                    childIndex++;
                }
            }
            CleanUpItems(firstVisibleIndex, current - 1);

            ComputeExtentAndViewport(availableSize, visibleSections);

            return availableSize;
        }
        protected override Size ArrangeOverride(Size finalSize)
        {
            if (_children != null)
            {
                foreach (UIElement child in _children)
                {
                    var layoutInfo = _realizedChildLayout[child];
                    child.Arrange(layoutInfo);
                }
            }
            return finalSize;
        }

        #endregion

        #region IScrollInfo Members

        private bool _canHScroll = false;
        public bool CanHorizontallyScroll
        {
            get { return _canHScroll; }
            set { _canHScroll = value; }
        }

        private bool _canVScroll = false;
        public bool CanVerticallyScroll
        {
            get { return _canVScroll; }
            set { _canVScroll = value; }
        }

        public double ExtentHeight
        {
            get { return _extent.Height; }
        }

        public double ExtentWidth
        {
            get { return _extent.Width; }
        }

        public double HorizontalOffset
        {
            get { return _offset.X; }
        }

        public double VerticalOffset
        {
            get { return _offset.Y; }
        }

        public void LineDown()
        {
            if (Orientation == Orientation.Vertical)
                SetVerticalOffset(VerticalOffset + 20);
            else
                SetVerticalOffset(VerticalOffset + 1);
        }

        public void LineLeft()
        {
            if (Orientation == Orientation.Horizontal)
                SetHorizontalOffset(HorizontalOffset - 20);
            else
                SetHorizontalOffset(HorizontalOffset - 1);
        }

        public void LineRight()
        {
            if (Orientation == Orientation.Horizontal)
                SetHorizontalOffset(HorizontalOffset + 20);
            else
                SetHorizontalOffset(HorizontalOffset + 1);
        }

        public void LineUp()
        {
            if (Orientation == Orientation.Vertical)
                SetVerticalOffset(VerticalOffset - 20);
            else
                SetVerticalOffset(VerticalOffset - 1);
        }

        public Rect MakeVisible(UIElement visual, Rect rectangle)
        {
            var gen = (ItemContainerGenerator)_generator.GetItemContainerGeneratorForPanel(this);
            var element = (UIElement)visual;
            int itemIndex = gen.IndexFromContainer(element);
            while (itemIndex == -1)
            {
                element = (UIElement)VisualTreeHelper.GetParent(element);
                itemIndex = gen.IndexFromContainer(element);
            }
            int section = _abstractPanel[itemIndex].Section;
            Rect elementRect = _realizedChildLayout[element];
            if (Orientation == Orientation.Horizontal)
            {
                double viewportHeight = _pixelMeasuredViewport.Height;
                if (elementRect.Bottom > viewportHeight)
                    _offset.Y += 1;
                else if (elementRect.Top < 0)
                    _offset.Y -= 1;
            }
            else
            {
                double viewportWidth = _pixelMeasuredViewport.Width;
                if (elementRect.Right > viewportWidth)
                    _offset.X += 1;
                else if (elementRect.Left < 0)
                    _offset.X -= 1;
            }
            InvalidateMeasure();
            return elementRect;
        }

        public void MouseWheelDown()
        {
            PageDown();
        }

        public void MouseWheelLeft()
        {
            PageLeft();
        }

        public void MouseWheelRight()
        {
            PageRight();
        }

        public void MouseWheelUp()
        {
            PageUp();
        }

        public void PageDown()
        {
            SetVerticalOffset(VerticalOffset + _viewport.Height * 0.8);
        }

        public void PageLeft()
        {
            SetHorizontalOffset(HorizontalOffset - _viewport.Width * 0.8);
        }

        public void PageRight()
        {
            SetHorizontalOffset(HorizontalOffset + _viewport.Width * 0.8);
        }

        public void PageUp()
        {
            SetVerticalOffset(VerticalOffset - _viewport.Height * 0.8);
        }

        private ScrollViewer _owner;
        public ScrollViewer ScrollOwner
        {
            get { return _owner; }
            set { _owner = value; }
        }

        public void SetHorizontalOffset(double offset)
        {
            if (offset < 0 || _viewport.Width >= _extent.Width)
            {
                offset = 0;
            }
            else
            {
                if (offset + _viewport.Width >= _extent.Width)
                {
                    offset = _extent.Width - _viewport.Width;
                }
            }

            _offset.X = offset;

            if (_owner != null)
                _owner.InvalidateScrollInfo();

            InvalidateMeasure();
            firstIndex = GetFirstVisibleIndex();
        }

        public void SetVerticalOffset(double offset)
        {
            if (offset < 0 || _viewport.Height >= _extent.Height)
            {
                offset = 0;
            }
            else
            {
                if (offset + _viewport.Height >= _extent.Height)
                {
                    offset = _extent.Height - _viewport.Height;
                }
            }

            _offset.Y = offset;

            if (_owner != null)
                _owner.InvalidateScrollInfo();

            //_trans.Y = -offset;

            InvalidateMeasure();
            firstIndex = GetFirstVisibleIndex();
        }

        public double ViewportHeight
        {
            get { return _viewport.Height; }
        }

        public double ViewportWidth
        {
            get { return _viewport.Width; }
        }

        #endregion

        #region helper data structures

        class ItemAbstraction
        {
            public ItemAbstraction(WrapPanelAbstraction panel, int index)
            {
                _panel = panel;
                _index = index;
            }

            WrapPanelAbstraction _panel;

            public readonly int _index;

            int _sectionIndex = -1;
            public int SectionIndex
            {
                get
                {
                    if (_sectionIndex == -1)
                    {
                        return _index % _panel._averageItemsPerSection - 1;
                    }
                    return _sectionIndex;
                }
                set
                {
                    if (_sectionIndex == -1)
                        _sectionIndex = value;
                }
            }

            int _section = -1;
            public int Section
            {
                get
                {
                    if (_section == -1)
                    {
                        return _index / _panel._averageItemsPerSection;
                    }
                    return _section;
                }
                set
                {
                    if (_section == -1)
                        _section = value;
                }
            }
        }

        class WrapPanelAbstraction : IEnumerable<ItemAbstraction>
        {
            public WrapPanelAbstraction(int itemCount)
            {
                List<ItemAbstraction> items = new List<ItemAbstraction>(itemCount);
                for (int i = 0; i < itemCount; i++)
                {
                    ItemAbstraction item = new ItemAbstraction(this, i);
                    items.Add(item);
                }

                Items = new ReadOnlyCollection<ItemAbstraction>(items);
                _averageItemsPerSection = itemCount;
                _itemCount = itemCount;
            }

            public readonly int _itemCount;
            public int _averageItemsPerSection;
            private int _currentSetSection = -1;
            private int _currentSetItemIndex = -1;
            private int _itemsInCurrentSecction = 0;
            private object _syncRoot = new object();

            public int SectionCount
            {
                get
                {
                    int ret = _currentSetSection + 1;
                    if (_currentSetItemIndex + 1 < Items.Count)
                    {
                        int itemsLeft = Items.Count - _currentSetItemIndex;
                        ret += itemsLeft / _averageItemsPerSection + 1;
                    }
                    return ret;
                }
            }

            private ReadOnlyCollection<ItemAbstraction> Items { get; set; }

            public void SetItemSection(int index, int section)
            {
                lock (_syncRoot)
                {
                    if (section <= _currentSetSection + 1 && index == _currentSetItemIndex + 1)
                    {
                        _currentSetItemIndex++;
                        Items[index].Section = section;
                        if (section == _currentSetSection + 1)
                        {
                            _currentSetSection = section;
                            if (section > 0)
                            {
                                _averageItemsPerSection = (index) / (section);
                            }
                            _itemsInCurrentSecction = 1;
                        }
                        else
                            _itemsInCurrentSecction++;
                        Items[index].SectionIndex = _itemsInCurrentSecction - 1;
                    }
                }
            }

            public ItemAbstraction this[int index]
            {
                get { return Items[index]; }
            }

            #region IEnumerable<ItemAbstraction> Members

            public IEnumerator<ItemAbstraction> GetEnumerator()
            {
                return Items.GetEnumerator();
            }

            #endregion

            #region IEnumerable Members

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            #endregion
        }

        #endregion
    }
}
