
class tableNavigationTrait<T> {

    constructor(private containerSelector: string, private currentItem: KnockoutObservable<T>, private allItems: KnockoutObservableArray<T> | KnockoutComputed<T[]>, private nThRowSelector: (idx: number) => string) {
        // empty by design
    }

    tableKeyDown(sender: any, e: KeyboardEvent) {
        const isKeyUp = e.keyCode === 38;
        const isKeyDown = e.keyCode === 40;
        if (isKeyUp || isKeyDown) {
            e.preventDefault();

            const oldSelection = this.currentItem();
            if (oldSelection) {
                const oldSelectionIndex = this.allItems().indexOf(oldSelection);
                let newSelectionIndex = oldSelectionIndex;
                if (isKeyUp && oldSelectionIndex > 0) {
                    newSelectionIndex--;
                } else if (isKeyDown && oldSelectionIndex < this.allItems().length - 1) {
                    newSelectionIndex++;
                }

                this.currentItem(this.allItems()[newSelectionIndex]);
                const newSelectedRow = $(this.nThRowSelector(newSelectionIndex + 1));
                if (newSelectedRow) {
                    this.ensureRowVisible(newSelectedRow);
                }
            }
            return false;
        }
        return true;
    }

    ensureRowVisible(row: JQuery) {
        const $container = $(this.containerSelector);
        const scrollTop = $container.scrollTop();
        const scrollBottom = scrollTop + $container.height();
        const scrollHeight = scrollBottom - scrollTop;

        const rowPosition = row.position();
        const rowTop = rowPosition.top;
        const rowBottom = rowTop + row.height();

        if (rowTop < 0) {
            $container.scrollTop(scrollTop + rowTop);
        } else if (rowBottom > scrollHeight) {
            $container.scrollTop(scrollTop + (rowBottom - scrollHeight));
        }
    }

}

export = tableNavigationTrait;
