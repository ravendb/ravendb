import DatabaseUtils from "components/utils/DatabaseUtils";

describe("DatabaseUtils", function () {
    describe("formatNameForFile", function () {
        it("throw when databaseName equal null", () => {
            expect(() =>
                DatabaseUtils.formatNameForFile(null, {
                    nodeTag: "A",
                    shardNumber: 2,
                })
            ).toThrow(Error("Must specify databaseName"));
        });

        it("correct format when not sharded", () => {
            const result = DatabaseUtils.formatNameForFile("dbName", null);

            expect(result).toEqual("dbName");
        });

        it("correct format when specified location without shardNumber", () => {
            const result = DatabaseUtils.formatNameForFile("dbName", {
                nodeTag: "C",
                shardNumber: null,
            });

            expect(result).toEqual("dbName_C");
        });

        it("correct format when specified location with shardNumber", () => {
            const result = DatabaseUtils.formatNameForFile("dbName", {
                nodeTag: "D",
                shardNumber: 0,
            });

            expect(result).toEqual("dbName_D_shard_0");
        });

        it("correct format when sharded without specified location", () => {
            const result = DatabaseUtils.formatNameForFile("dbName$3", null);

            expect(result).toEqual("dbName_shard_3");
        });
    });

    describe("namesEqualIgnoreCase", function () {
        it("true when names differ only in casing", () => {
            expect(DatabaseUtils.namesEqualIgnoreCase("Northwind", "northWIND")).toBe(true);
        });

        it("false when names differ", () => {
            expect(DatabaseUtils.namesEqualIgnoreCase("Northwind", "orders")).toBe(false);
        });

        it("false when any name is not provided", () => {
            expect(DatabaseUtils.namesEqualIgnoreCase("Northwind", null)).toBe(false);
            expect(DatabaseUtils.namesEqualIgnoreCase(null, null)).toBe(false);
        });
    });
});
