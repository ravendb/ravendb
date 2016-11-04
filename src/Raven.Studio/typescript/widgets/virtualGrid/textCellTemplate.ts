import cellTemplate = require("widgets/virtualGrid/cellTemplate");

class textCellTemplate implements cellTemplate {
    className = "text-cell";

    getHtml(item: Object, dataMemberName: string, isSelected: boolean): string {
        const cellValue = (item as any)[dataMemberName];
        if (cellValue) {
            return cellValue.toString();
        }

        return "";
    }
}

export = textCellTemplate;
