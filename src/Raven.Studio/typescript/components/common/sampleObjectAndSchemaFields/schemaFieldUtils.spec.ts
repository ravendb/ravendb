import { schemaFieldUtils } from "./schemaFieldUtils";

const {
    getIsStrictFromSaved,
    getIsStrictToSave,
    getHasSampleObject,
    getHasSampleObjectOrWrittenSchema,
    getWrittenSchemaOrNull,
} = schemaFieldUtils;

const SAMPLE_OBJECT = '{ "orderId": "The ID of the order" }';
const WRITTEN_SCHEMA = '{ "type": "object" }';
const BLANK_SCHEMA = " \n";
const BLANK_SAMPLE_OBJECT = " \n";

describe("schemaFieldUtils", () => {
    it("loads a tool without a written schema as strict, because the server sends a generated schema in strict mode", () => {
        expect(getIsStrictFromSaved(null, false)).toBe(true);
        expect(getIsStrictFromSaved(null, undefined)).toBe(true);
    });

    it("loads a tool with a written schema with its saved value, and as not strict when it was saved before strict mode existed", () => {
        expect(getIsStrictFromSaved(WRITTEN_SCHEMA, true)).toBe(true);
        expect(getIsStrictFromSaved(WRITTEN_SCHEMA, false)).toBe(false);
        expect(getIsStrictFromSaved(WRITTEN_SCHEMA, undefined)).toBe(false);
    });

    it("does not save strict mode for a tool without a written schema, so a plain save does not change the tool", () => {
        expect(getIsStrictToSave("", true)).toBe(false);
    });

    it("treats a schema with only white space as not written, like the server does", () => {
        expect(getIsStrictFromSaved(BLANK_SCHEMA, false)).toBe(true);
        expect(getIsStrictToSave(BLANK_SCHEMA, true)).toBe(false);
    });

    it("does not accept or save a schema with only white space, because the server rejects a tool without a sample object and a schema", () => {
        expect(getHasSampleObjectOrWrittenSchema("", BLANK_SCHEMA)).toBe(false);
        expect(getHasSampleObjectOrWrittenSchema(SAMPLE_OBJECT, BLANK_SCHEMA)).toBe(true);
        expect(getHasSampleObjectOrWrittenSchema("", WRITTEN_SCHEMA)).toBe(true);
        expect(getWrittenSchemaOrNull(BLANK_SCHEMA)).toBeNull();
        expect(getWrittenSchemaOrNull(WRITTEN_SCHEMA)).toBe(WRITTEN_SCHEMA);
    });

    it("treats a sample object with only white space as missing, so the schema cannot be regenerated from it", () => {
        expect(getHasSampleObject(BLANK_SAMPLE_OBJECT)).toBe(false);
        expect(getHasSampleObject(SAMPLE_OBJECT)).toBe(true);
    });

    it("saves the chosen strict mode for a tool with a written schema", () => {
        expect(getIsStrictToSave(WRITTEN_SCHEMA, true)).toBe(true);
        expect(getIsStrictToSave(WRITTEN_SCHEMA, false)).toBe(false);
    });
});
