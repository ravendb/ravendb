/// <reference path="../../typings/tsd.d.ts" />

class idGenerator {
    public static generateId(idLength = 5) {
        var text = "";
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (var i = 0; i < idLength; i++)
            text += chars.charAt(Math.floor(Math.random() * chars.length));

        return text;
    }
} 

export = idGenerator
