import CellTemplate = require("widgets/virtualGrid/CellTemplate");

class TextCellTemplate implements CellTemplate {
    className = "text-cell";

    getHtml(item: Object, dataMemberName: string, isSelected: boolean): string {
        var cellValue = (item as any)[dataMemberName];
        if (cellValue) {
            return cellValue.toString();
        }

        return "";
    }
}

export = TextCellTemplate;