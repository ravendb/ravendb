import storageKeyProvider from "common/storage/storageKeyProvider";
import { documentsColumnLayoutStorage } from "components/pages/database/documents/documentsList/utils/documentsColumnLayoutStorage";
import document from "models/database/documents/document";

const DATABASE_NAME = "db1";
const COLLECTION_NAME = "Orders";

const storageKey = storageKeyProvider.storageKeyFor(`custom-columns-${DATABASE_NAME}.[${COLLECTION_NAME}]`);

describe("documentsColumnLayoutStorage", () => {
    beforeEach(() => {
        localStorage.clear();
    });

    it("drops a stored layout that is malformed or has an unexpected shape", () => {
        localStorage.setItem(storageKey, "{not json");
        expect(documentsColumnLayoutStorage.load(DATABASE_NAME, COLLECTION_NAME)).toBeNull();

        localStorage.setItem(storageKey, JSON.stringify({ visibleColumnIds: "Company" }));
        expect(documentsColumnLayoutStorage.load(DATABASE_NAME, COLLECTION_NAME)).toBeNull();
        expect(localStorage.getItem(storageKey)).toBeNull();
    });

    it("migrates the layout saved by the previous documents page", () => {
        localStorage.setItem(
            storageKey,
            JSON.stringify([
                { visible: true, editable: false, column: { type: "checkbox", header: null, serializedValue: null } },
                {
                    visible: true,
                    editable: false,
                    column: { type: "hyperlink", header: "Id", serializedValue: document.customColumnName },
                },
                {
                    visible: false,
                    editable: false,
                    column: { type: "text", header: "Freight", serializedValue: "Freight" },
                },
                {
                    visible: true,
                    editable: false,
                    column: { type: "text", header: "Company", serializedValue: "Company" },
                },
                {
                    visible: true,
                    editable: true,
                    column: { type: "custom", header: "Ship&#x2F;City", serializedValue: "this.ShipTo.City" },
                },
                { visible: true, editable: false, column: { type: "flags", header: null, serializedValue: null } },
            ])
        );

        const layout = documentsColumnLayoutStorage.load(DATABASE_NAME, COLLECTION_NAME);
        const [customColumn] = layout.customColumns;

        expect(customColumn).toMatchObject({ header: "Ship/City", expression: "this.ShipTo.City" });
        expect(layout.columnOrder).toEqual(["@id", "property:Freight", "property:Company", customColumn.id, "Flags"]);
        expect(layout.visibleColumnIds).toEqual(["@id", "property:Company", customColumn.id, "Flags"]);
        expect(documentsColumnLayoutStorage.load(DATABASE_NAME, COLLECTION_NAME)).toEqual(layout);
    });
});
