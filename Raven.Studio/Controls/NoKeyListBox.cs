namespace Raven.Studio.Controls {
    using System.Windows.Controls;
    using System.Windows.Input;

    public class NoKeyListBox : ListBox {
        protected override void OnKeyDown(KeyEventArgs e) {}

        protected override void OnKeyUp(KeyEventArgs e) {}
    }
}