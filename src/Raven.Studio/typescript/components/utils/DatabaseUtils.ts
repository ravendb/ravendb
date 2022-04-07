import { data } from "jquery";

export default class DatabaseUtils {
    
    static isSharded(name: string) {
        return name.includes("$");
    }


    static extractName(name: string) {
        return DatabaseUtils.isSharded(name) ? name.split("$")[0] : name;
    }
}
