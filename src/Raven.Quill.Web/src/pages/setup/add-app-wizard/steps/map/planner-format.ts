export function formatCount(count: number, singular: string, plural = `${singular}s`): string {
    return `${count} ${count === 1 ? singular : plural}`;
}

export function interleaveListSeparators<Item>(items: Item[]): (Item | string)[] {
    return items.flatMap((item, index) => {
        if (index === 0) {
            return [item];
        }

        return [index === items.length - 1 ? " and " : ", ", item];
    });
}

export function joinList(items: string[]): string {
    return interleaveListSeparators(items).join("");
}

export function upperFirst(text: string): string {
    return text.charAt(0).toUpperCase() + text.slice(1);
}
