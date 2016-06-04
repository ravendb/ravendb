
class tableNavigationTrait<T> {

    constructor(private containerSelector: string, private currentItem: KnockoutObservable<T>, private allItems: KnockoutObservableArray<T> | KnockoutComputed<T[]>, private nThRowSelector: (int) => string) {
        // empty by design
    }

    tableKeyDown(sender: any, e: KeyboardEvent) {
        var isKeyUp = e.keyCode === 38;
        var isKeyDown = e.keyCode === 40;
        if (isKeyUp || isKeyDown) {
            e.preventDefault();

            var oldSelection = this.currentItem();
            if (oldSelection) {
                var oldSelectionIndex = this.allItems().indexOf(oldSelection);
                var newSelectionIndex = oldSelectionIndex;
                if (isKeyUp && oldSelectionIndex > 0) {
                    newSelectionIndex--;
                } else if (isKeyDown && oldSelectionIndex < this.allItems().length - 1) {
                    newSelectionIndex++;
                }

                this.currentItem(this.allItems()[newSelectionIndex]);
                var newSelectedRow = $(this.nThRowSelector(newSelectionIndex + 1));
                if (newSelectedRow) {
                    this.ensureRowVisible(newSelectedRow);
                }
            }
            return false;
        }
        return true;
    }

    ensureRowVisible(row: JQuery) {
        var $container = $(this.containerSelector);
        var scrollTop = $container.scrollTop();
        var scrollBottom = scrollTop + $container.height();
        var scrollHeight = scrollBottom - scrollTop;

        var rowPosition = row.position();
        var rowTop = rowPosition.top;
        var rowBottom = rowTop + row.height();

        if (rowTop < 0) {
            $container.scrollTop(scrollTop + rowTop);
        } else if (rowBottom > scrollHeight) {
            $container.scrollTop(scrollTop + (rowBottom - scrollHeight));
        }
    }

}

export = tableNavigationTrait;
