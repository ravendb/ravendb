interface CellTemplate {
    getHtml(item: Object, dataMemberName: string, isSelected: boolean): string;
    className: string;
}

export = CellTemplate;