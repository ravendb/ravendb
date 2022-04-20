export default class DatabaseUtils {
    static isSharded(name: string) {
        return name.includes("$");
    }

    static shardGroupKey(name: string) {
        return DatabaseUtils.isSharded(name) ? name.split("$")[0] : name;
    }

    static shardNumber(name: string): number {
        return parseInt(name.split("$")[1], 10);
    }
}
