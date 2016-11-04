interface cellTemplate {
    getHtml(item: Object, dataMemberName: string, isSelected: boolean): string;
    className: string;
}

export = cellTemplate;
