namespace Raven.Studio.Framework
{
    public class MenuItemMetadata : IMenuItemMetadata
    {
        public MenuItemMetadata(string displayName, int index)
        {
            DisplayName = displayName;
            Index = index;
        }

        public string DisplayName { get; private set; }
        public int Index { get; private set; }
    }
}