/// <reference path="../../../typings/globals/knockout/index.d.ts" />
import cellTemplate = require("widgets/virtualGrid/cellTemplate");

class checkedCellTemplate implements cellTemplate {
    className = "checked-cell";
    isChecked = ko.observable(false);

    static readonly columnWidth = 32;
    static readonly checkedDataMember = "__virtual-grid-isChecked";

    getHtml(item: Object, dataMemberName: string, isSelected: boolean): string {
        if (isSelected) {
            return `<input class="checked-cell-input" type="checkbox" checked />`;
        }

        return `<input class="checked-cell-input" type="checkbox" />`;
    }
}

export = checkedCellTemplate;
